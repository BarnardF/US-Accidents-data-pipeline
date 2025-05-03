"""
DAG to load fixed US Accidents data from GCS to BigQuery
This DAG loads monthly accident data chunks with fixed End_Lat and End_Lng values
from GCS to BigQuery with appropriate schema definition, partitioning, and clustering.
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from google.cloud import storage, bigquery
from datetime import datetime, timedelta
import logging
import json
import os

# Constants
GCS_BUCKET = Variable.get("gcs_bucket_name")
GCS_FIXED_PATH = "processed/monthly"
BQ_DATASET = "us_accidents_data"
BQ_RAW_TABLE = "fixed_accidents"

# Google project ID
try:
    PROJECT_ID = Variable.get("gcp_project_id")
except Exception as e:
    logging.warning(f"Could not retrieve project ID from Airflow Variable: {e}")

# Load BigQuery schema from file
def load_schema_from_file():
    """Load BigQuery schema from JSON file"""
    schema_file_path = 'data/processed/schema/bigquery_schema.json'
    with open(schema_file_path, 'r') as schema_file:
        schema = json.load(schema_file)
    logging.info(f"Loaded schema with {len(schema)} fields from schema file")
    return schema

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def check_for_files():
    """
    Check if there are any fixed files in GCS to load.
    Returns the appropriate task to run next.
    """
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    blobs = list(bucket.list_blobs(prefix=GCS_FIXED_PATH))
    
    csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
    
    if csv_files:
        logging.info(f"Found {len(csv_files)} fixed CSV files to load")
        return "create_bq_dataset"
    else:
        logging.warning("No fixed CSV files found in GCS bucket")
        return "no_files_found"

def log_file_list():
    """Log the list of fixed files in GCS that will be loaded"""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    blobs = list(bucket.list_blobs(prefix=GCS_FIXED_PATH))
    
    csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
    
    logging.info("Fixed files to be loaded into BigQuery:")
    for file in csv_files:
        logging.info(f"- {file}")
    
    return csv_files

with DAG(
    dag_id="us_accidents_load_fixed_files",
    default_args=default_args,
    description="Load fixed US Accidents data from GCS to BigQuery",
    schedule_interval=None,  # Manual trigger
    start_date=days_ago(1),
    catchup=False,
    tags=["us_accidents", "bigquery"],
) as dag:

    # Check if there are files to process
    check_files = BranchPythonOperator(
        task_id="check_for_files",
        python_callable=check_for_files,
    )
    
    # Task for when no files are found
    no_files_found = DummyOperator(
        task_id="no_files_found",
    )
    
    # List files that will be processed
    list_files = PythonOperator(
        task_id="list_files_to_load",
        python_callable=log_file_list,
    )
    
    # Create BigQuery dataset if it doesn't exist
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=BQ_DATASET,
        project_id=PROJECT_ID,
        exists_ok=True,
    )
    
    # Create BigQuery table if it doesn't exist
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bq_table",
        project_id=PROJECT_ID,
        dataset_id=BQ_DATASET,
        table_id=BQ_RAW_TABLE,
        schema_fields=load_schema_from_file(),  # Use function to load schema
        exists_ok=True,
        time_partitioning={
            "type": "MONTH",
            "field": "Start_Time",
        },
        cluster_fields=["State", "Severity"],
    )
    
    # Load data from GCS to BigQuery (using fixed files)
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_fixed_files_to_bq",
        bucket=GCS_BUCKET,
        source_objects=[f"{GCS_FIXED_PATH}/*.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}",
        schema_fields=load_schema_from_file(),
        source_format="CSV",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,  # Handle potential missing fields
        max_bad_records=10000000,    
        autodetect=False,        # We're using a defined schema
        field_delimiter=",",     # Explicitly specify comma as delimiter
        quote_character='"',     # Explicitly specify quote character
        ignore_unknown_values=True, # Ignore values that don't match schema
    )
    
    # Run a data quality check
    data_quality_check = BigQueryExecuteQueryOperator(
        task_id="data_quality_check",
        sql=f"""
        SELECT
            COUNT(*) AS total_records,
            COUNTIF(End_Lat IS NULL) AS null_end_lat_count,
            COUNTIF(End_Lng IS NULL) AS null_end_lng_count,
            MIN(Start_Time) AS earliest_date,
            MAX(Start_Time) AS latest_date,
            COUNT(DISTINCT State) AS state_count
        FROM
            `{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}`
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    )
    
    # Log success message
    success_msg = PythonOperator(
        task_id="log_success",
        python_callable=lambda: logging.info("Successfully loaded fixed data into BigQuery"),
    )

    # Define workflow
    check_files >> no_files_found
    check_files >> create_dataset >> create_table >> list_files >> load_to_bq >> data_quality_check >> success_msg