from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(dag_id='kafka_streaming_moodle', start_date=datetime(2025, 1, 1), schedule_interval='@daily', catchup=False)
def kafka_streaming_moodle():
    submit_kafka_streaming_moodle = SparkSubmitOperator(
        task_id='submit_kafka_streaming_moodle',
        application='include/kafka_streaming_moodle.py',
        conn_id='spark_conn',
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        driver_memory='1g',
        verbose=True,
        name='kafka_streaming_moodle',
        conf={
            "spark.streaming.stopGracefullyOnShutdown": "true"
        }
    )


kafka_streaming_moodle()
