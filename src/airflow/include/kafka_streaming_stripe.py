import configparser
import json
import requests
from datetime import datetime, timezone, timedelta

import uuid
import pandas as pd
import pymysql
import redis
from sshtunnel import SSHTunnelForwarder
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max as max_, from_json, expr, window, avg, count,
    broadcast, row_number, from_unixtime
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType
from pyspark.sql.window import Window


# Send a Telegram alert using bot token and chat ID
def send_telegram_alert(message, chat_id, token):
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": message
        }
        response = requests.post(url, data=data)
        if not response.ok:
            print(f"[Telegram] Failed to send alert: {response.text}")
    except Exception as e:
        print(f"[Telegram Error] {e}")


# Update Kafka checkpoint offsets in INI file
def update_checkpoint(config_path, topic_key, new_partition_offsets):
    config = configparser.ConfigParser()
    config.read(config_path)
    if not config.has_section('kafka_local'):
        config.add_section('kafka_local')
    current_checkpoint = {}
    try:
        if config.has_option('kafka_local', f"{topic_key}_checkpoint_offset"):
            current_checkpoint = json.loads(config.get('kafka_local', f"{topic_key}_checkpoint_offset"))
    except:
        pass
    for partition, offset in new_partition_offsets.items():
        current_checkpoint[partition] = offset
    config.set('kafka_local', f"{topic_key}_checkpoint_offset", json.dumps(current_checkpoint))
    with open(config_path, 'w') as configfile:
        config.write(configfile)


# Retrieve Kafka checkpoint offsets from config
def get_checkpoint(config, topic_key):
    try:
        checkpoint = config.get('kafka_local', f"{topic_key}_checkpoint_offset", fallback=None)
        if checkpoint:
            offsets = json.loads(checkpoint)
            if isinstance(offsets, dict) and len(offsets) > 0:
                return offsets
        return "earliest"
    except:
        return "earliest"


# Load user, course, and enrol lookup tables from MySQL via SSH tunnel
def load_lookup_data(db_conf):
    with SSHTunnelForwarder(
        (db_conf.get('ssh_host'), int(db_conf.get('ssh_port'))),
        ssh_username=db_conf.get('ssh_user'),
        ssh_pkey=db_conf.get('ssh_key'),
        remote_bind_address=(db_conf.get('remote_bind_host'), int(db_conf.get('remote_bind_port'))),
        local_bind_address=('127.0.0.1', 0)
    ) as tunnel:
        local_port = tunnel.local_bind_port
        conn = pymysql.connect(
            host='127.0.0.1',
            port=local_port,
            user=db_conf.get('db_user'),
            password=db_conf.get('db_password'),
            database=db_conf.get('db_name')
        )

        course_df = pd.read_sql("SELECT id AS course_id, fullname AS course_name FROM mdl_course WHERE format = 'topics'", conn)
        enrol_df = pd.read_sql("SELECT id AS enrol_id, courseid AS course_id FROM mdl_enrol", conn)
        enrol_df = enrol_df.merge(course_df, on='course_id', how='left')
        return course_df, enrol_df


if __name__ == '__main__':
    CONFIG_PATH = 'config/kafka.ini'
    TOPIC_KEY = 'stripe_events'

    # Load configuration from INI file
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    kafka_conf = config['kafka_local']
    topic_name = kafka_conf.get('charge_succeeded')
    current_offsets = get_checkpoint(config, TOPIC_KEY)

    telegram_bot_config = config['telegram_bot']
    TELEGRAM_CHAT_ID = telegram_bot_config.get('TELEGRAM_CHAT_ID')
    TELEGRAM_TOKEN = telegram_bot_config.get('TELEGRAM_TOKEN')

    # Kafka connection options for Spark
    kafka_options = {
        "kafka.bootstrap.servers": kafka_conf.get('bootstrap_servers'),
        "kafka.security.protocol": kafka_conf.get('security_protocol'),
        "kafka.sasl.mechanism": kafka_conf.get('sasl_mechanism'),
        "kafka.sasl.jaas.config": kafka_conf.get('sasl_jaas_config'),
        "subscribe": topic_name,
        "startingOffsets": "earliest" if current_offsets == "earliest" else json.dumps({topic_name: current_offsets}),
        "kafka.group.id": "stripe_streaming_consumer"
    }

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("StripeKafkaToRedisGrafana") \
        .master("spark://spark:7077") \
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
        .getOrCreate()

    # Load lookup tables from Moodle DB config
    db_conf = config['ssh_mysql']

    # Connect to Redis
    redis_client = redis.Redis(host='airflow-redis-1', port=6379, decode_responses=True)

    # Define schema for Stripe Kafka message
    schema = StructType([
        StructField("id", StringType()),
        StructField("eventname", StringType()),
        StructField("created", LongType()),
        StructField("data", StructType([
            StructField("object", StructType([
                StructField("amount", StringType()),
                StructField("metadata", StructType([
                    StructField("itemid", StringType()),
                    StructField("userid", StringType())
                ])),
            ]))
        ]))
    ])

    # Read Kafka stream as DataFrame
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    # Parse JSON from Kafka value field
    value_df = df.selectExpr("CAST(value AS STRING)", "topic", "partition", "offset") \
        .withColumn("json_data", from_json(col("value"), schema)) \
        .select(
            col("json_data.id").alias("id"),
            col("json_data.eventname").alias("eventname"),
            col("json_data.created").alias("created"),
            (col("json_data.data.object.amount").cast("long") / 100).alias("amount"),
            col("json_data.data.object.metadata.userid").alias("userid"),
            col("json_data.data.object.metadata.itemid").alias("itemid"),
            col("partition"), col("offset")
        ) \
        .withColumn("created", from_unixtime(col("created")).cast("timestamp"))

    # =======================
    # STREAM 1: SLIDING ALERT
    # =======================

    # Define Vietnam timezone
    VN_TZ = timezone(timedelta(hours=7))

    # Create a sliding window aggregation on event time with watermarking
    # - Watermark: 2 minutes
    # - Window duration: 5 minutes
    # - Sliding interval: 1 minute
    event_window_df = value_df \
        .withWatermark("created", "2 minutes") \
        .groupBy(window(col("created"), "5 minutes", "1 minute")) \
        .agg(count("*").alias("event_count"))

    def process_alert_window(df, batch_id):
        # Threshold for sending an alert
        alert_threshold = 1

        # Filter windows that exceed the threshold
        alert_df = df.filter(col("event_count") > alert_threshold)

        # Collect the filtered windows and format alert messages
        alerts = []
        for row in alert_df.collect():
            window_start = row["window"]["start"].replace(tzinfo=timezone.utc).astimezone(VN_TZ)
            window_end = row["window"]["end"].replace(tzinfo=timezone.utc).astimezone(VN_TZ)
            count = row["event_count"]
            alerts.append(f"• {count} events from {window_start} to {window_end}")

        # Send alert via Telegram if there are any alerts
        if alerts:
            message = "[Sliding Stripe Alert]\n" + "\n".join(alerts)
            send_telegram_alert(message, TELEGRAM_CHAT_ID, TELEGRAM_TOKEN)

    alert_query = event_window_df.writeStream \
        .outputMode("update") \
        .foreachBatch(process_alert_window) \
        .trigger(processingTime="10 seconds") \
        .start()


    # ==========================
    # STREAM 2: MAIN PROCESSING
    # ==========================

    # Define batch processing logic for each micro-batch from Kafka
    def process_batch(df, batch_id, db_conf):
        if df.isEmpty():
            print(f"[BATCH {batch_id}] No data.")
            return

        # Reload lookup data from Moodle database
        course_df, enrol_df = load_lookup_data(db_conf)
        spark_enrol_df = spark.createDataFrame(enrol_df)

        # Join event data with enrolment lookup
        df = df.join(broadcast(spark_enrol_df), df["itemid"] == spark_enrol_df["enrol_id"], how="left")

        # Store each purchase amount into Redis with 24h TTL
        for row in df.select("amount").collect():
            amount = row["amount"]
            if amount is not None:
                unique_key = f"purchase_amount:{uuid.uuid4().hex}"
                redis_client.setex(unique_key, 86400, amount)

        # Aggregate all purchase amounts in Redis
        total_amount = 0.0
        for key in redis_client.scan_iter("purchase_amount:*"):
            try:
                amt = float(redis_client.get(key) or 0)
                total_amount += amt
            except:
                continue

        redis_client.set("total_purchase_amount_24h", total_amount)
        redis_client.expire("total_purchase_amount_24h", 86400)

        # Save purchase events per course to compute top 3 most purchased courses
        for row in df.select("course_id", "course_name", "userid", "created").collect():
            cid = row['course_id']
            cname = row['course_name']
            uid = row['userid']
            ts = int(row['created'].timestamp())

            if cid and cname and uid and ts:
                raw_key = f"course_purchase:{cid}:uids"
                redis_client.sadd(raw_key, f"{uid}:{ts}")
                redis_client.expire(raw_key, 86400)
                # redis_client.setex(f"course_purchase:{cid}:name", 86400, cname)

        # Update top 3 most purchased courses ranking in Redis
        redis_client.delete("top_course_purchases_formatted")
        for row in course_df.itertuples():
            cid = row.course_id
            cname = row.course_name
            key = f"course_purchase:{cid}:uids"
            count = redis_client.scard(key)
            if count > 0:
                display = f"{cname} ({count} purchases)"
                redis_client.zadd("top_course_purchases_formatted", {display: count})
        redis_client.expire("top_course_purchases_formatted", 86400)

        # Update Kafka checkpoint offsets after processing
        new_offsets = {}
        for row in df.groupBy("partition").agg(max_("offset")).collect():
            new_offsets[str(row["partition"])] = row["max(offset)"] + 1
        update_checkpoint(CONFIG_PATH, TOPIC_KEY, new_offsets)
        print(f"[BATCH {batch_id}] Checkpoint updated.\n")


    # Start Spark Structured Streaming query
    query = value_df.writeStream \
        .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, db_conf)) \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()