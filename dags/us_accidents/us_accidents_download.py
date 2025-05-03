"""
DAG to download US Accidents dataset from Kaggle
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import os
import json
import logging
import subprocess
import zipfile
import glob


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'us_accidents_download',
    default_args=default_args,
    description='Download US Accidents dataset from Kaggle',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['us_accidents'],
)


def setup_kaggle_credentials(**kwargs):
    """Create Kaggle API credentials file"""
    try:
        # Try to get credentials from Airflow Variables
        try:
            kaggle_username = Variable.get("kaggle_username")
            kaggle_key = Variable.get("kaggle_key")
            logging.info("Retrieved Kaggle credentials from Airflow Variables")
        except Exception as e:
            logging.warning(f"Could not retrieve Kaggle credentials from Variables: {e}")
            # Fallback to hardcoded credentials (only for development)
            # Replace these with your actual Kaggle credentials for testing
            kaggle_username = "kaggle_username"
            kaggle_key = "kaggle_key" 
            logging.warning("Using fallback credentials - replace with your own!")
        
        # Create .kaggle directory if it doesn't exist
        os.makedirs('/opt/airflow/.kaggle', exist_ok=True)
        
        # Create kaggle.json file with credentials
        with open('/opt/airflow/.kaggle/kaggle.json', 'w') as f:
            json.dump({'username': kaggle_username, 'key': kaggle_key}, f)
        
        # Set proper permissions
        os.chmod('/opt/airflow/.kaggle/kaggle.json', 0o600)
        
        return "Kaggle credentials set up successfully"
    except Exception as e:
        logging.error(f"Failed to set up Kaggle credentials: {e}")
        raise

setup_creds = PythonOperator(
    task_id='setup_kaggle_credentials',
    python_callable=setup_kaggle_credentials,
    dag=dag,
)



def install_kaggle_package(**kwargs):
    """Install Kaggle package if not already installed"""
    import subprocess
    try:
        # Check if kaggle is already installed
        subprocess.check_call(['pip', 'list', '|', 'grep', 'kaggle'], shell=True)
        logging.info("Kaggle package already installed")
    except subprocess.CalledProcessError:
        # Install kaggle package
        logging.info("Installing Kaggle package")
        subprocess.check_call(['pip', 'install', 'kaggle'])
    
    return "Kaggle package checked/installed"

install_kaggle = PythonOperator(
    task_id='install_kaggle_package',
    python_callable=install_kaggle_package,
    dag=dag,
)



def download_us_accidents(**kwargs):
    """Download and unzip the US Accidents dataset from Kaggle"""
    download_dir = '/opt/airflow/data/raw'
    os.makedirs(download_dir, exist_ok=True)
    os.environ['KAGGLE_CONFIG_DIR'] = '/opt/airflow/.kaggle'

    logging.info("Downloading dataset from Kaggle...")
    subprocess.run([
        'kaggle', 'datasets', 'download', 
        '-d', 'sobhanmoosavi/us-accidents',
        '--force', '-p', download_dir
    ], check=True)

    zip_path = os.path.join(download_dir, 'us-accidents.zip')
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"{zip_path} not found after download")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(download_dir)
    os.remove(zip_path)
    logging.info("Dataset unzipped and cleaned up")

download_dataset = PythonOperator(
    task_id='download_dataset',
    python_callable=download_us_accidents,
    dag=dag,
)



check_file = BashOperator(
    task_id='check_file',
    bash_command='''
    echo "Checking downloaded files:"
    ls -la /opt/airflow/data/raw
    file_count=$(ls -1 /opt/airflow/data/raw/*.csv 2>/dev/null | wc -l)
    if [ $file_count -gt 0 ]; then
        echo "Found $file_count CSV files. Dataset downloaded successfully."
        # Print file sizes
        du -h /opt/airflow/data/raw/*.csv
    else
        echo "No CSV files found in the directory!"
        exit 1
    fi
    ''',
    dag=dag,
)


# Task dependencies
install_kaggle >> setup_creds >> download_dataset >> check_file