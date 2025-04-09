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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
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
    
    kaggle_username = Variable.get("kaggle_username")
    kaggle_key = Variable.get("kaggle_key")  
    
    # Create .kaggle directory if it doesn't exist
    os.makedirs('/opt/airflow/.kaggle', exist_ok=True)
    
    # Create kaggle.json file with credentials
    with open('/opt/airflow/.kaggle/kaggle.json', 'w') as f:
        json.dump({'username': kaggle_username, 'key': kaggle_key}, f)
    
    # Set proper permissions
    os.chmod('/opt/airflow/.kaggle/kaggle.json', 0o600)
    
    return "Kaggle credentials set up successfully"

setup_creds = PythonOperator(
    task_id='setup_kaggle_credentials',
    python_callable=setup_kaggle_credentials,
    dag=dag,
)

# Command to download the dataset
# Note: This is just for demonstration; you'll need to provide your Kaggle credentials
download_dataset = BashOperator(
    task_id='download_dataset',
    bash_command='''
    mkdir -p /opt/airflow/data/raw
    cd /opt/airflow/data/raw
    kaggle datasets download -d sobhanmoosavi/us-accidents
    unzip -o us-accidents.zip
    rm us-accidents.zip
    ''',
    dag=dag,
)

check_file = BashOperator(
    task_id='check_file',
    bash_command='ls -la /opt/airflow/data/raw && echo "Dataset downloaded successfully"',
    dag=dag,
)

setup_creds >> download_dataset >> check_file
