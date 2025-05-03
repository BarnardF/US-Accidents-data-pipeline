"""
DAG to fix NULL values in End_Lat and End_Lng columns in GCS CSV files

This DAG processes monthly accident data chunks stored in GCS,
fixing NULL values in End_Lat and End_Lng by replacing them with
corresponding Start_Lat and Start_Lng values, then writes the fixed
files back to GCS.
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
from datetime import datetime, timedelta
import logging
import pandas as pd
import io
import os

# Constants
GCS_BUCKET = Variable.get("gcs_bucket_name")
GCS_MONTHLY_PATH = "raw/monthly"
GCS_FIXED_PATH = "processed/monthly"  # Path for fixed files

# Google project ID
try:
    PROJECT_ID = Variable.get("gcp_project_id")
except Exception as e:
    logging.warning(f"Could not retrieve project ID from Airflow Variable: {e}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def list_csv_files():
    """List all CSV files in the GCS bucket path"""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    blobs = list(bucket.list_blobs(prefix=GCS_MONTHLY_PATH))
    
    csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
    
    if csv_files:
        logging.info(f"Found {len(csv_files)} CSV files to process")
        for file in csv_files:
            logging.info(f"- {file}")
        return csv_files
    else:
        logging.warning("No CSV files found in GCS bucket")
        return []

def process_file(gcs_file_path):
    """Process a single CSV file to fix NULL values"""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    # Get the blob
    blob = bucket.blob(gcs_file_path)
    
    # Get the file name for the output path
    file_name = os.path.basename(gcs_file_path)
    output_path = f"{GCS_FIXED_PATH}/{file_name}"
    
    try:
        # Download the file content
        content = blob.download_as_text()
        
        # Load into pandas DataFrame
        df = pd.read_csv(io.StringIO(content), low_memory=False)
        
        # Count nulls before fixing
        null_end_lat_before = df['End_Lat'].isna().sum()
        null_end_lng_before = df['End_Lng'].isna().sum()
        
        logging.info(f"File {file_name}: Found {null_end_lat_before} null End_Lat values and {null_end_lng_before} null End_Lng values")
        
        # Fix NULL values by replacing with Start coordinates
        df['End_Lat'] = df['End_Lat'].fillna(df['Start_Lat'])
        df['End_Lng'] = df['End_Lng'].fillna(df['Start_Lng'])
        
        # Count nulls after fixing
        null_end_lat_after = df['End_Lat'].isna().sum()
        null_end_lng_after = df['End_Lng'].isna().sum()
        
        # Convert back to CSV
        fixed_content = df.to_csv(index=False)
        
        # Upload the fixed file
        output_blob = bucket.blob(output_path)
        output_blob.upload_from_string(fixed_content, content_type='text/csv')
        
        logging.info(f"Fixed file uploaded to {output_path}")
        logging.info(f"File {file_name}: Reduced null End_Lat from {null_end_lat_before} to {null_end_lat_after}")
        logging.info(f"File {file_name}: Reduced null End_Lng from {null_end_lng_before} to {null_end_lng_after}")
        
        return {
            'file': file_name,
            'null_end_lat_before': null_end_lat_before,
            'null_end_lat_after': null_end_lat_after,
            'null_end_lng_before': null_end_lng_before,
            'null_end_lng_after': null_end_lng_after
        }
        
    except Exception as e:
        logging.error(f"Error processing file {gcs_file_path}: {str(e)}")
        raise

def process_all_files(ti):
    """Process all CSV files from the list"""
    csv_files = ti.xcom_pull(task_ids='list_csv_files')
    
    if not csv_files:
        logging.warning("No files to process")
        return
    
    results = []
    for file_path in csv_files:
        try:
            result = process_file(file_path)
            results.append(result)
        except Exception as e:
            logging.error(f"Failed to process {file_path}: {str(e)}")
    
    # Summarize results
    total_files = len(results)
    total_nulls_fixed_lat = sum(r['null_end_lat_before'] - r['null_end_lat_after'] for r in results)
    total_nulls_fixed_lng = sum(r['null_end_lng_before'] - r['null_end_lng_after'] for r in results)
    
    logging.info(f"Processed {total_files} files")
    logging.info(f"Fixed {total_nulls_fixed_lat} null End_Lat values")
    logging.info(f"Fixed {total_nulls_fixed_lng} null End_Lng values")
    
    return results

def create_fixed_folder_if_needed():
    """Create the processed/monthly folder in GCS if it doesn't exist"""
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    # Check if folder exists by listing blobs with the prefix
    blobs = list(bucket.list_blobs(prefix=GCS_FIXED_PATH, max_results=1))
    
    if not blobs:
        # Folder doesn't exist, create an empty file to establish the folder
        empty_blob = bucket.blob(f"{GCS_FIXED_PATH}/.keep")
        empty_blob.upload_from_string('')
        logging.info(f"Created folder {GCS_FIXED_PATH} in bucket {GCS_BUCKET}")
    else:
        logging.info(f"Folder {GCS_FIXED_PATH} already exists in bucket {GCS_BUCKET}")

with DAG(
    dag_id="us_accidents_fix_null_coordinates",
    default_args=default_args,
    description="Fix NULL values in End_Lat and End_Lng columns in GCS CSV files",
    schedule_interval=None,  # Manual trigger
    start_date=days_ago(1),
    catchup=False,
    tags=["us_accidents", "data_cleaning"],
) as dag:

    # Create the output folder if needed
    create_folder = PythonOperator(
        task_id="create_fixed_folder",
        python_callable=create_fixed_folder_if_needed,
    )
    
    # List files to process
    list_files = PythonOperator(
        task_id="list_csv_files",
        python_callable=list_csv_files,
    )
    
    # Process all files
    process_files = PythonOperator(
        task_id="process_all_files",
        python_callable=process_all_files,
    )
    
    # Define workflow
    create_folder >> list_files >> process_files