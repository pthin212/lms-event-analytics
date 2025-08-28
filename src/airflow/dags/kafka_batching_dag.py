from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

# Path to the dbt project directory inside the Airflow container
DBT_DIR = "/opt/airflow/include/dbt/the25_bigquery_dbt"


def create_migration_task(table_name: str) -> BashOperator:
    """
    Create a BashOperator task that runs the Moodle migration script
    for a specific table.

    Args:
        table_name (str): Name of the Moodle table to migrate.

    Returns:
        BashOperator: Airflow task that executes the migration.
    """
    return BashOperator(
        task_id=f'moodle_migrate_{table_name}',
        bash_command=f'python3 /opt/airflow/include/moodle_migration.py {table_name}',
    )


@dag(
    dag_id='kafka_batching',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
)
def kafka_batching():
    """
    DAG for orchestrating Kafka batching, Moodle migrations, and dbt transformations.
    The workflow:
        1. Submits a Spark job for Kafka batching.
        2. Migrates Moodle tables sequentially.
        3. Runs dbt models for staging.
        4. Runs dbt models for marts.
    """

    # Step 1: Submit Spark job to batch data from Kafka
    submit_kafka_batching = SparkSubmitOperator(
        task_id='submit_kafka_batching',
        application='include/kafka_batching.py',
        conn_id='spark_conn',
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        ),
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='1g',
        num_executors='1',
        driver_memory='1g',
        verbose=True,
        jars='config/gcs-connector-hadoop3-latest.jar',
        name='kafka_batching',
    )

    # Step 2: Run Moodle migration tasks for individual tables
    moodle_migrate_course = create_migration_task("mdl_course")
    moodle_migrate_user = create_migration_task("mdl_user")
    moodle_migrate_enrol = create_migration_task("mdl_enrol")
    moodle_migrate_discussions = create_migration_task("mdl_forum_discussions")
    moodle_migrate_course_categories = create_migration_task("mdl_course_categories")

    # Step 3: Run dbt models for staging layer
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --target staging --select tag:staging --profiles-dir .",
    )

    # Step 4: Run dbt models for marts layer
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --target marts --select tag:marts --profiles-dir .",
    )

    # Define task dependencies / execution order
    (submit_kafka_batching \
        >> moodle_migrate_course >> moodle_migrate_user >> moodle_migrate_enrol >> moodle_migrate_discussions \
        >> moodle_migrate_course_categories >> dbt_run_staging >> dbt_run_marts)


# Instantiate the DAG
kafka_batching()
