"""
DAG to simulate batch processing of US Accidents data month by month
starting from 2016, as if we're processing historical data over time.
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
import logging
import glob

# Setup logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  # Changed to False to prevent blocking if a task fails
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Constants and paths
CHUNKS_DIR = '/opt/airflow/data/processed/chunks'
GCS_BUCKET = Variable.get('gcs_bucket_name')
GCS_MONTHLY_PATH = 'raw/monthly'

# This represents our "simulation" start date
# We'll start processing data from January 2016
# Each DAG run will process one month's data
SIMULATION_START_DATE = datetime(2016, 1, 1)

# Define the DAG
dag = DAG(
    'us_accidents_simulate_monthly_processing',
    default_args=default_args,
    description='Simulate batch processing of US Accidents data month by month',
    # Run once a day (in a real scenario, you might run weekly or monthly)
    # For simulation purposes, each day will represent one month of data
    schedule_interval=timedelta(days=1),
    start_date=SIMULATION_START_DATE,
    catchup=True,  # Process all months since start date
    max_active_runs=1,  # Process only one month at a time in sequence
    tags=['us_accidents', 'simulation'],
)

def check_available_months():
    """
    Find all available months by scanning the directory for files
    """
    # Get all files matching the pattern
    file_pattern = os.path.join(CHUNKS_DIR, "us_accidents_*.csv")
    all_files = glob.glob(file_pattern)
    
    # Extract month strings from filenames
    available_months = []
    for file_path in all_files:
        filename = os.path.basename(file_path)
        # Extract YYYY-MM part
        if filename.startswith("us_accidents_") and filename.endswith(".csv"):
            month_str = filename[len("us_accidents_"):-4]  # Remove prefix and .csv
            available_months.append(month_str)
    
    logger.info(f"Found {len(available_months)} available months: {available_months}")
    return sorted(available_months)

def get_month_to_process(**kwargs):
    """
    Determine which month's data to process based on execution date or manual override
    """
    # Check if a specific month was provided in dag_run.conf
    dag_run = kwargs.get('dag_run')
    if dag_run and dag_run.conf and 'month' in dag_run.conf:
        month_str = dag_run.conf['month']
        logger.info(f"Using manually specified month: {month_str}")
    else:
        # Get the execution date from the context
        execution_date = kwargs['execution_date']
        # Format as YYYY-MM for finding the corresponding file
        month_str = execution_date.strftime("%Y-%m")
        logger.info(f"Using execution date month: {month_str}")
    
    # File naming pattern
    filename = f"us_accidents_{month_str}.csv"
    file_path = os.path.join(CHUNKS_DIR, filename)
    
    logger.info(f"Processing month: {month_str}")
    logger.info(f"Looking for file: {file_path}")
    
    # Check if file exists
    if not os.path.exists(file_path):
        logger.warning(f"File not found for month {month_str}: {file_path}")
        return None
    
    # Return details about the file to process
    return {
        'month': month_str,
        'filename': filename,
        'file_path': file_path
    }

def check_file_exists(**kwargs):
    """
    Check if the file exists for the month to be processed.
    Returns True if file exists, False otherwise.
    This is a short circuit operator that will skip downstream tasks if False.
    """
    ti = kwargs['ti']
    month_info = ti.xcom_pull(task_ids='get_month_to_process')
    
    if not month_info:
        logger.warning("No month info available, skipping this run")
        return False
    
    file_path = month_info['file_path']
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}, skipping this run")
        return False
    
    logger.info(f"File exists: {file_path}, continuing with processing")
    return True

def upload_monthly_file_to_gcs(**kwargs):
    """
    Upload the monthly file to Google Cloud Storage
    """
    # Get task instance
    ti = kwargs['ti']
    
    # Get month information from previous task
    month_info = ti.xcom_pull(task_ids='get_month_to_process')
    
    # This should never happen due to the short circuit, but just in case
    if not month_info:
        logger.warning("No file information provided. Skipping upload.")
        return "Skipped - No file found"
    
    file_path = month_info['file_path']
    filename = month_info['filename']
    month = month_info['month']
    
    # Double-check if file exists (redundant after short circuit but safe)
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return f"Skipped - File not found: {filename}"
    
    # Calculate file size
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    
    # Target path in GCS
    gcs_path = f"{GCS_MONTHLY_PATH}/{filename}"
    
    try:
        # Upload to GCS
        logger.info(f"Uploading {filename} ({file_size_mb:.2f} MB) to GCS: gs://{GCS_BUCKET}/{gcs_path}")
        
        # Use GCS Hook for upload
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
        gcs_hook.upload(
            bucket_name=GCS_BUCKET,
            object_name=gcs_path,
            filename=file_path
        )
        
        logger.info(f"Successfully uploaded {filename} to GCS")
        
        # Return success info
        return {
            "month": month,
            "filename": filename,
            "gcs_path": f"gs://{GCS_BUCKET}/{gcs_path}",
            "file_size_mb": round(file_size_mb, 2),
            "status": "success"
        }
    except Exception as e:
        logger.error(f"Error uploading {filename} to GCS: {str(e)}")
        # Raising the exception will make the task fail
        # and potentially retry based on your retry settings
        raise

def log_upload_results(**kwargs):
    """
    Log the results of the upload
    """
    # Get task instance
    ti = kwargs['ti']
    
    # Get upload results from previous task
    upload_results = ti.xcom_pull(task_ids='upload_monthly_file_to_gcs')
    
    # Log based on result type
    if isinstance(upload_results, dict) and upload_results.get('status') == 'success':
        logger.info(f"Month {upload_results['month']} processed successfully")
        logger.info(f"File uploaded to: {upload_results['gcs_path']}")
        logger.info(f"File size: {upload_results['file_size_mb']} MB")
        return f"Successfully processed month: {upload_results['month']}"
    else:
        logger.info(f"Month processing result: {upload_results}")
        return f"Process completed with message: {upload_results}"

# Create tasks
get_month_task = PythonOperator(
    task_id='get_month_to_process',
    python_callable=get_month_to_process,
    provide_context=True,
    dag=dag,
)

# Add short circuit to skip if file doesn't exist
check_file_task = ShortCircuitOperator(
    task_id='check_file_exists',
    python_callable=check_file_exists,
    provide_context=True,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_monthly_file_to_gcs',
    python_callable=upload_monthly_file_to_gcs,
    provide_context=True,
    dag=dag,
)

log_results_task = PythonOperator(
    task_id='log_upload_results',
    python_callable=log_upload_results,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
get_month_task >> check_file_task >> upload_task >> log_results_task