import configparser
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_, from_json, expr, from_unixtime
from pyspark.sql.types import *
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import math


def update_checkpoint(config_path, topic_key, new_partition_offsets):
    config = configparser.ConfigParser()
    config.read(config_path)

    # Create kafka_local section if it doesn't exist
    if not config.has_section('kafka_local'):
        config.add_section('kafka_local')

    # Load current checkpoint if exists
    current_checkpoint = {}
    try:
        if config.has_option('kafka_local', f"{topic_key}_checkpoint_offset"):
            current_checkpoint = json.loads(config.get('kafka_local', f"{topic_key}_checkpoint_offset"))
    except:
        pass

    # Update offsets with new values
    for partition, offset in new_partition_offsets.items():
        current_checkpoint[partition] = offset

    # Save updated checkpoint to config file
    config.set('kafka_local', f"{topic_key}_checkpoint_offset", json.dumps(current_checkpoint))

    with open(config_path, 'w') as configfile:
        config.write(configfile)


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


if __name__ == '__main__':
    # Load configuration from file
    config = configparser.ConfigParser()
    config.read('config/batching_conf.ini')
    kafka_conf = config['kafka_local']

    # Define Kafka topics to process
    topics = {
        "moodle_events": kafka_conf.get("topic_moodle_events"),
        "stripe_events": kafka_conf.get("charge_succeeded"),
        "error_things": kafka_conf.get("topic_error"),
    }

    spark = SparkSession.builder \
        .appName("KafkaCheckpointToGCS") \
        .master("spark://spark:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/tmp/thesis-25-456305-079e7bf41602.json") \
        .getOrCreate()

    # Process each topic
    for topic_key, topic_name in topics.items():
        print(f"\n=== Processing topic: {topic_name} ===")
        current_offsets = get_checkpoint(config, topic_key)
        print(f"Current checkpoint: {current_offsets}")

        kafka_options = {
            "kafka.bootstrap.servers": kafka_conf.get('bootstrap_servers'),
            "kafka.security.protocol": kafka_conf.get('security_protocol'),
            "kafka.sasl.mechanism": kafka_conf.get('sasl_mechanism'),
            "kafka.sasl.jaas.config": kafka_conf.get('sasl_jaas_config'),
            "subscribe": topic_name,
            "startingOffsets": "earliest" if current_offsets == "earliest" else json.dumps({topic_name: current_offsets}),
            "kafka.group.id": "batching_consumer"
        }

        df = spark.read.format("kafka").options(**kafka_options).load()

        if df.rdd.isEmpty():
            print("No new data found")
        else:
            # Process Stripe events differently due to complex schema
            if topic_name == "stripe_events":
                stripe_schema = StructType([
                    StructField("id", StringType()),
                    StructField("object", StringType()),
                    StructField("api_version", StringType()),
                    StructField("created", LongType()),  # UNIX timestamp
                    StructField("type", StringType()),
                    StructField("livemode", BooleanType()),
                    StructField("pending_webhooks", LongType()),
                    StructField("request", StructType([
                        StructField("id", StringType()),
                        StructField("idempotency_key", StringType())
                    ])),
                    StructField("data", StructType([
                        StructField("object", StructType([
                            StructField("id", StringType()),
                            StructField("object", StringType()),
                            StructField("amount", LongType()),
                            StructField("amount_captured", LongType()),
                            StructField("amount_refunded", LongType()),
                            StructField("application", StringType()),
                            StructField("application_fee", StringType()),
                            StructField("application_fee_amount", LongType()),
                            StructField("balance_transaction", StringType()),
                            StructField("billing_details", StructType([
                                StructField("address", StructType([
                                    StructField("city", StringType()),
                                    StructField("country", StringType()),
                                    StructField("line1", StringType()),
                                    StructField("line2", StringType()),
                                    StructField("postal_code", StringType()),
                                    StructField("state", StringType())
                                ])),
                                StructField("email", StringType()),
                                StructField("name", StringType()),
                                StructField("phone", StringType()),
                                StructField("tax_id", StringType())
                            ])),
                            StructField("calculated_statement_descriptor", StringType()),
                            StructField("captured", BooleanType()),
                            StructField("created", LongType()),
                            StructField("currency", StringType()),
                            StructField("customer", StringType()),
                            StructField("description", StringType()),
                            StructField("destination", StringType()),
                            StructField("dispute", StringType()),
                            StructField("disputed", BooleanType()),
                            StructField("failure_balance_transaction", StringType()),
                            StructField("failure_code", StringType()),
                            StructField("failure_message", StringType()),
                            StructField("livemode", BooleanType()),
                            StructField("metadata", StructType([
                                StructField("itemid", StringType()),
                                StructField("lastname", StringType()),
                                StructField("component", StringType()),
                                StructField("userid", StringType()),
                                StructField("firstname", StringType()),
                                StructField("paymentarea", StringType()),
                                StructField("username", StringType())
                            ])),
                            StructField("on_behalf_of", StringType()),
                            StructField("order", StringType()),
                            StructField("outcome", StructType([
                                StructField("advice_code", StringType()),
                                StructField("network_advice_code", StringType()),
                                StructField("network_decline_code", StringType()),
                                StructField("network_status", StringType()),
                                StructField("reason", StringType()),
                                StructField("risk_level", StringType()),
                                StructField("risk_score", LongType()),
                                StructField("seller_message", StringType()),
                                StructField("type", StringType())
                            ])),
                            StructField("paid", BooleanType()),
                            StructField("payment_intent", StringType()),
                            StructField("payment_method", StringType()),
                            StructField("payment_method_details", StructType([
                                StructField("card", StructType([
                                    StructField("amount_authorized", LongType()),
                                    StructField("authorization_code", StringType()),
                                    StructField("brand", StringType()),
                                    StructField("checks", StructType([
                                        StructField("address_line1_check", StringType()),
                                        StructField("address_postal_code_check", StringType()),
                                        StructField("cvc_check", StringType())
                                    ])),
                                    StructField("country", StringType()),
                                    StructField("exp_month", LongType()),
                                    StructField("exp_year", LongType()),
                                    StructField("extended_authorization", StructType([
                                        StructField("status", StringType())
                                    ])),
                                    StructField("fingerprint", StringType()),
                                    StructField("funding", StringType()),
                                    StructField("incremental_authorization", StructType([
                                        StructField("status", StringType())
                                    ])),
                                    StructField("installments", StringType()),
                                    StructField("last4", StringType()),
                                    StructField("mandate", StringType()),
                                    StructField("multicapture", StructType([
                                        StructField("status", StringType())
                                    ])),
                                    StructField("network", StringType()),
                                    StructField("network_token", StructType([
                                        StructField("used", BooleanType())
                                    ])),
                                    StructField("network_transaction_id", StringType()),
                                    StructField("overcapture", StructType([
                                        StructField("maximum_amount_capturable", LongType()),
                                        StructField("status", StringType())
                                    ])),
                                    StructField("regulated_status", StringType()),
                                    StructField("three_d_secure", StringType()),
                                    StructField("wallet", StringType())
                                ])),
                                StructField("type", StringType())
                            ])),
                            StructField("presentment_details", StructType([
                                StructField("presentment_amount", LongType()),
                                StructField("presentment_currency", StringType())
                            ])),
                            StructField("receipt_email", StringType()),
                            StructField("receipt_number", StringType()),
                            StructField("receipt_url", StringType()),
                            StructField("refunded", BooleanType()),
                            StructField("review", StringType()),
                            StructField("shipping", StringType()),
                            StructField("source", StringType()),
                            StructField("source_transfer", StringType()),
                            StructField("statement_descriptor", StringType()),
                            StructField("statement_descriptor_suffix", StringType()),
                            StructField("status", StringType()),
                            StructField("transfer_data", StringType()),
                            StructField("transfer_group", StringType())
                        ]))
                    ]))
                ])

                parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
                    .withColumn("data", from_json(col("json_str"), stripe_schema)) \
                    .select("data.*") \
                    .withColumn("data", col("data").withField(
                        "object", col("data.object").withField("_order", col("data.object.order"))
                        .dropFields("order")
                    )) \
                    .withColumn("created", from_unixtime(col("created")).cast("timestamp"))

            else:
                schema = StructType([
                    StructField("eventname", StringType()),
                    StructField("component", StringType()),
                    StructField("action", StringType()),
                    StructField("target", StringType()),
                    StructField("objecttable", StringType()),
                    StructField("objectid", LongType()),
                    StructField("crud", StringType()),
                    StructField("edulevel", LongType()),
                    StructField("contextid", LongType()),
                    StructField("contextlevel", LongType()),
                    StructField("contextinstanceid", LongType()),
                    StructField("userid", LongType()),
                    StructField("courseid", LongType()),
                    StructField("relateduserid", LongType()),
                    StructField("anonymous", BooleanType()),
                    StructField("other", StructType([
                        StructField("username", StringType()),
                        StructField("sessionid", StringType())
                    ])),
                    StructField("timecreated", TimestampType()),
                    StructField("host", StringType()),
                    StructField("extra", StringType())
                ])

                parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
                    .withColumn("data", from_json(col("json_str"), schema)) \
                    .select("data.*")

            # Display sample data and schema
            parsed_df.show(5, truncate=False)
            parsed_df.printSchema()

            # Prepare output path with timestamp partition
            now_vn = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
            timestamp_str = now_vn.strftime("date=%Y-%m-%d/hour=%H")
            output_path = f"gs://thesis25_{topic_key}/{timestamp_str}"
            # output_path = f"gs://test_bucket_mistyday/{topic_key}/{timestamp_str}"

            # Write data to GCS in Parquet format
            record_count = parsed_df.count()

            # 10 records / file
            records_per_file = 10
            num_output_files = max(1, math.ceil(record_count / records_per_file))

            # Repartitioning
            parsed_df = parsed_df.repartition(num_output_files)

            parsed_df.write \
                .mode("append") \
                .parquet(output_path)

            # Calculate new offsets to update checkpoint
            new_offsets = {}
            for row in df.groupBy("partition").agg(max_("offset")).collect():
                partition = str(row["partition"])
                new_offsets[partition] = row["max(offset)"] + 1

            print(f"New offsets to update: {new_offsets}")
            update_checkpoint('config/batching_conf.ini', topic_key, new_offsets)

    spark.stop()