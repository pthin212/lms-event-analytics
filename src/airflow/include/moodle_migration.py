import os
import sys
import time
import pymysql
import pandas as pd
import configparser
from sshtunnel import SSHTunnelForwarder
from google.cloud import bigquery


config = configparser.ConfigParser()
config.read("/opt/airflow/config/config.ini")

# SSH & MySQL configuration
SSH_HOST = config.get("ssh_mysql", "ssh_host")
SSH_PORT = config.getint("ssh_mysql", "ssh_port")
SSH_USER = config.get("ssh_mysql", "ssh_user")
SSH_KEY_PATH = config.get("ssh_mysql", "ssh_key")
REMOTE_BIND_HOST = config.get("ssh_mysql", "remote_bind_host")
REMOTE_BIND_PORT = config.getint("ssh_mysql", "remote_bind_port")

MYSQL_HOST = "127.0.0.1"  # local forwarding host
MYSQL_USER = config.get("ssh_mysql", "db_user")
MYSQL_PASSWORD = config.get("ssh_mysql", "db_password")
MYSQL_DB = config.get("ssh_mysql", "db_name")

# BigQuery configuration
BQ_PROJECT = config.get("bigquery", "project_id", fallback="thesis-25-456305")
BQ_DATASET = config.get("bigquery", "dataset", fallback="raw_layer")

# Service account credentials for BigQuery
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.get(
    "bigquery",
    "credentials_path",
    fallback="/opt/airflow/config/thesis-25-456305-079e7bf41602.json"
)

# Retry policy
MAX_RETRIES = 3
DELAY = 1  # base delay in seconds


# Get table name from CLI arguments
table_name = sys.argv[1] if len(sys.argv) > 1 else None
if not table_name:
    raise ValueError("Missing table name as argument.")

BQ_TABLE = table_name


# Main migration logic
for attempt in range(1, MAX_RETRIES + 1):
    try:
        print(f"Attempt {attempt} to open SSH tunnel...")

        # Open SSH tunnel to remote MySQL server
        with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_pkey=SSH_KEY_PATH,
            remote_bind_address=(REMOTE_BIND_HOST, REMOTE_BIND_PORT),
            local_bind_address=(MYSQL_HOST, 0)  # bind to a free local port
        ) as tunnel:
            local_port = tunnel.local_bind_port

            # Connect to MySQL through the SSH tunnel
            connection = pymysql.connect(
                host=MYSQL_HOST,
                port=local_port,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=MYSQL_DB,
            )

            # Fetch data from the given table
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, connection)
            print(f"Fetched {len(df)} rows from {table_name}")

            # Initialize BigQuery client and define target table
            bq_client = bigquery.Client(project=BQ_PROJECT)
            table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

            # Load dataframe into BigQuery (overwrite mode)
            job = bq_client.load_table_from_dataframe(
                df,
                table_id,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
            )
            job.result()
            print(f"Uploaded to BigQuery: {table_id}")

            break

    except Exception as e:
        print(f"Failed on attempt {attempt}: {e}")
        if attempt < MAX_RETRIES:
            time.sleep(DELAY * (2 ** (attempt - 1)))
        else:
            raise
