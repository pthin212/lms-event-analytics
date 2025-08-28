import configparser
import json
import requests
from datetime import datetime, timezone, timedelta

import pandas as pd
import pymysql
import redis
from sshtunnel import SSHTunnelForwarder
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max as max_, from_json, expr, window, avg, count,
    broadcast, row_number
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


# Load user, course, and discussion lookup tables from MySQL via SSH tunnel
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
        user_df = pd.read_sql("SELECT id AS user_id, CONCAT(firstname, ' ', lastname) AS fullname FROM mdl_user", conn)
        course_df = pd.read_sql("SELECT id AS course_id, fullname AS course_name FROM mdl_course WHERE format = 'topics'", conn)
        discussion_df = pd.read_sql("SELECT id AS discussion_id, name AS discussion_title FROM mdl_forum_discussions", conn)
        return user_df, course_df, discussion_df


if __name__ == '__main__':
    CONFIG_PATH = 'config/kafka.ini'
    TOPIC_KEY = 'moodle_events'

    # Load config from INI file
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    kafka_conf = config['kafka_local']
    topic_name = kafka_conf.get('topic_moodle_events')
    current_offsets = get_checkpoint(config, TOPIC_KEY)

    telegram_bot_config = config['telegram_bot']
    TELEGRAM_CHAT_ID = telegram_bot_config.get('TELEGRAM_CHAT_ID')
    TELEGRAM_TOKEN = telegram_bot_config.get('TELEGRAM_TOKEN')

    # Kafka configuration for Spark
    kafka_options = {
        "kafka.bootstrap.servers": kafka_conf.get('bootstrap_servers'),
        "kafka.security.protocol": kafka_conf.get('security_protocol'),
        "kafka.sasl.mechanism": kafka_conf.get('sasl_mechanism'),
        "kafka.sasl.jaas.config": kafka_conf.get('sasl_jaas_config'),
        "subscribe": topic_name,
        "startingOffsets": "earliest" if current_offsets == "earliest" else json.dumps({topic_name: current_offsets}),
        "kafka.group.id": "moodle_streaming_consumer"
    }

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("KafkaToRedisGrafanaOptimized") \
        .master("spark://spark:7077") \
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
        .getOrCreate()

    # Load lookup tables and convert to Spark DataFrame
    db_conf = config['ssh_mysql']

    # Connect to Redis
    redis_client = redis.Redis(host='airflow-redis-1', port=6379, decode_responses=True)

    # Define schema for Kafka message
    schema = StructType([
        StructField("eventname", StringType()),
        StructField("action", StringType()),
        StructField("userid", LongType()),
        StructField("courseid", LongType()),
        StructField("objectid", LongType()),
        StructField("timecreated", TimestampType())
    ])

    # Read from Kafka stream
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    # Extract value and parse JSON
    value_df = df.selectExpr("CAST(value AS STRING)", "topic", "partition", "offset") \
        .withColumn("json_data", from_json(col("value"), schema)) \
        .select("json_data.*", "partition", "offset")

    # =======================
    # STREAM 1: SLIDING ALERT
    # =======================

    # Define Vietnam timezone
    VN_TZ = timezone(timedelta(hours=7))

    # Create a sliding window aggregation on event time with watermarking
    # -  : 2 minutes
    # - Window duration: 5 minutes
    # - Sliding interval: 1 minute
    event_window_df = value_df \
        .withWatermark("timecreated", "2 minutes") \
        .groupBy(window(col("timecreated"), "5 minutes", "1 minute")) \
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
            message = "[Sliding Moodle Alert]\n" + "\n".join(alerts)
            send_telegram_alert(message, TELEGRAM_CHAT_ID, TELEGRAM_TOKEN)

    alert_query = event_window_df.writeStream \
        .outputMode("update") \
        .foreachBatch(process_alert_window) \
        .trigger(processingTime="10 seconds") \
        .start()


    # ==========================
    # STREAM 2: MAIN PROCESSING
    # ==========================

    # Process each batch of Kafka events
    def process_batch(df, batch_id, db_conf):
        if df.isEmpty():
            print(f"[BATCH {batch_id}] No data.")
            return

        # Reload lookup data from Moodle database
        user_df, course_df, discussion_df = load_lookup_data(db_conf)
        spark_user_df = spark.createDataFrame(user_df)
        spark_course_df = spark.createDataFrame(course_df)
        spark_discussion_df = spark.createDataFrame(discussion_df)

        # Enrich with lookup info
        df = df \
            .join(broadcast(spark_user_df), df["userid"] == spark_user_df["user_id"], how="left") \
            .join(broadcast(spark_course_df), df["courseid"] == spark_course_df["course_id"], how="left") \
            .join(broadcast(spark_discussion_df), df["objectid"] == spark_discussion_df["discussion_id"], how="left")

        # Define event types
        login_event = "\\core\\event\\user_loggedin"
        logout_event = "\\core\\event\\user_loggedout"
        course_view_event = "\\core\\event\\course_viewed"
        discussion_view_event = "\\mod_forum\\event\\discussion_viewed"

        course_view_counts = {}
        discussion_view_counts = {}

        for row in df.collect():
            ts = int(row['timecreated'].timestamp())
            uid = row['userid']
            uname = row['fullname']
            cid = row['courseid']
            cname = row['course_name']
            did = row['objectid']
            dtitle = row['discussion_title']
            event = row['eventname']

            # Handle login event
            if event == login_event:
                redis_client.setbit("online_bitmap", uid, 1)
                redis_client.expire("online_bitmap", 86400)
                redis_client.sadd("online_users_list", uname)
                redis_client.expire("online_users_list", 86400)

            # Handle logout event
            elif event == logout_event:
                redis_client.setbit("online_bitmap", uid, 0)
                redis_client.expire("online_bitmap", 86400)
                redis_client.srem("online_users_list", uname)

            # Handle course view event
            elif event == course_view_event and cid and cname:
                raw_key = f"course:{cid}:views_24h"
                redis_client.sadd(raw_key, f"{uid}:{ts}")
                redis_client.expire(raw_key, 86400)
                course_view_counts[cname] = raw_key

                redis_client.sadd("course_view_total_24h", f"{uid}:{ts}")
                redis_client.expire("course_view_total_24h", 86400)

                # redis_client.zadd("course_view_trend", {cname: ts})
                # redis_client.expire("course_view_trend", 86400)

            # Handle discussion view event
            elif event == discussion_view_event and did and dtitle:
                raw_key = f"discussion:{did}:views_24h"
                redis_client.sadd(raw_key, f"{uid}:{ts}")
                redis_client.expire(raw_key, 86400)
                discussion_view_counts[dtitle] = raw_key

            # Store all events for total event count in 24h
            redis_client.sadd("all_moodle_events_24h", f"{event}:{uid}:{ts}")
            redis_client.expire("all_moodle_events_24h", 86400)

        # Update top viewed courses
        redis_client.delete("top_course_views_formatted")
        for row in course_df.itertuples():
            cid = row.course_id
            cname = row.course_name
            key = f"course:{cid}:views_24h"
            count = redis_client.scard(key)
            if count > 0:
                display = f"{cname} ({count} views)"
                redis_client.zadd("top_course_views_formatted", {display: count})
        redis_client.expire("top_course_views_formatted", 86400)

        # Update top viewed discussions
        redis_client.delete("top_discussion_views_formatted")
        for row in discussion_df.itertuples():
            did = row.discussion_id
            dtitle = row.discussion_title
            key = f"discussion:{did}:views_24h"
            count = redis_client.scard(key)
            if count > 0:
                display = f"{dtitle} ({count} views)"
                redis_client.zadd("top_discussion_views_formatted", {display: count})
        redis_client.expire("top_discussion_views_formatted", 86400)

        # Update Kafka offsets as checkpoint
        new_offsets = {}
        for row in df.groupBy("partition").agg(max_("offset")).collect():
            new_offsets[str(row["partition"])] = row["max(offset)"] + 1
        update_checkpoint(CONFIG_PATH, TOPIC_KEY, new_offsets)
        print(f"[BATCH {batch_id}] Checkpoint updated.\n")


    # Start Spark streaming query
    query = value_df.writeStream \
        .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, db_conf)) \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()