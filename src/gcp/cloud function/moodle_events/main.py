import functions_framework
from google.cloud import bigquery, storage
from google.api_core.exceptions import GoogleAPIError


@functions_framework.cloud_event
def cf_moodle_events(cloud_event):
    data = cloud_event.data

    bucket_name = data["bucket"]
    file_name = data["name"]

    print("Cloud Function triggered")
    print(f"Bucket: {bucket_name}")
    print(f"File: {file_name}")

    # Skip folder markers
    if file_name.endswith('/'):
        print(f"Skipping folder marker: {file_name}")
        return

    # Only trigger on _SUCCESS files
    if not file_name.endswith('_SUCCESS'):
        print(f"Not a _SUCCESS marker. Skipping file: {file_name}")
        return

    # Determine prefix to locate .parquet files
    prefix = '/'.join(file_name.split('/')[:-1]) + '/'
    print(f"Looking for parquet files in folder: gs://{bucket_name}/{prefix}")

    # BigQuery table information
    project_id = "thesis-25-456305"
    dataset_id = "raw_layer"
    table_id = "moodle_events"
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Initialize GCP clients
    bq_client = bigquery.Client(project=project_id)
    storage_client = storage.Client()

    # Get all .parquet files in the current prefix
    blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix))
    parquet_blobs = [b for b in blobs if b.name.endswith('.parquet')]

    print(f"Detected {len(parquet_blobs)} .parquet files.")

    if not parquet_blobs:
        print("No .parquet files found. Exiting.")
        return

    # Configure BigQuery job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    # Determine topic_key and target bucket
    topic_key = bucket_name.replace("thesis25_", "")
    processed_bucket_name = "thesis25_processed"

    source_bucket = storage_client.bucket(bucket_name)
    destination_bucket = storage_client.bucket(processed_bucket_name)

    # Process and move each .parquet file
    for i, blob in enumerate(parquet_blobs, start=1):
        gcs_uri = f"gs://{bucket_name}/{blob.name}"
        print(f"Processing file {i}: {gcs_uri} (size: {blob.size / 1024:.2f} KB)")

        try:
            # Load to BigQuery
            load_job = bq_client.load_table_from_uri(
                gcs_uri,
                table_ref,
                job_config=job_config,
            )
            print(f"Started BigQuery load job {load_job.job_id} for file {i}")
            load_job.result()
            print(f"Successfully loaded file {i}")

            # Move file to processed bucket (preserve full path)
            dest_blob_name = f"{topic_key}/{blob.name}"
            source_bucket.copy_blob(blob, destination_bucket, dest_blob_name)
            blob.delete()
            print(f"Moved to: gs://{processed_bucket_name}/{dest_blob_name}")

        except GoogleAPIError as e:
            print(f"Google API error while loading file {i}: {e.message}")
        except Exception as e:
            print(f"Unexpected error while loading file {i}: {str(e)}")

    print("All files have been loaded and moved.")