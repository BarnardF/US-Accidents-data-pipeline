"""
DAG for exploring the US Accidents dataset and preparing schema for BigQuery
"""
from datetime import datetime, timedelta
import os
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


RAW_DATA_PATH = '/opt/airflow/data/raw/US_Accidents_March23.csv'
SCHEMA_PATH = '/opt/airflow/data/processed/schema'
PROFILE_PATH = '/opt/airflow/data/processed/profile'
os.makedirs(os.path.dirname(SCHEMA_PATH), exist_ok=True)
os.makedirs(os.path.dirname(PROFILE_PATH), exist_ok=True)


def sample_data():
    """Read a sample of the dataset to speed up exploration"""
    # Read only the first 10 000 rows for initial exploration
    df = pd.read_csv(RAW_DATA_PATH, nrows=10000)
    
    sample_path = '/opt/airflow/data/processed/us_accidents_sample.csv'
    df.to_csv(sample_path, index=False)
    
    return sample_path


def explore_data(ti):
    """Perform basic data profiling"""
    sample_path = ti.xcom_pull(task_ids='sample_data')
    df = pd.read_csv(sample_path)
    
    # basic statistics
    profile = {
        'row_count': len(df),
        'columns': list(df.columns),
        'missing_values': df.isnull().sum().to_dict(),
        'data_types': df.dtypes.astype(str).to_dict(),
        'numeric_stats': {}
    }
    
    #stats for numeric columns
    for col in df.select_dtypes(include=['number']).columns:
        profile['numeric_stats'][col] = {
            'min': df[col].min(),
            'max': df[col].max(),
            'mean': df[col].mean(),
            'median': df[col].median(),
            'std': df[col].std()
        }
    
    with open(f"{PROFILE_PATH}/data_profile.json", 'w') as f:
        json.dump(profile, f, indent=2, default=str)
    
    return profile


def generate_bigquery_schema(ti):
    """Generate BigQuery schema from dataset"""
    sample_path = ti.xcom_pull(task_ids='sample_data')
    df = pd.read_csv(sample_path)
    
    # Map pandas dtypes to BigQuery data types
    dtype_mapping = {
        'int64': 'INTEGER',
        'float64': 'FLOAT',
        'object': 'STRING',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
    
    schema = []
    for column, dtype in df.dtypes.items():
        dtype_str = str(dtype)
        
        # Convert to datetime if the column looks like a date
        if 'date' in column.lower() or 'time' in column.lower():
            try:
                # Try to convert to datetime
                df[column] = pd.to_datetime(df[column])
                dtype_str = 'datetime64[ns]'
            except:
                pass
                
        bq_type = dtype_mapping.get(dtype_str, 'STRING')
        
        # Create field schema
        field = {
            "name": column,
            "type": bq_type,
            "mode": "NULLABLE" if df[column].isnull().any() else "REQUIRED"
        }
        schema.append(field)
    
    with open(f"{SCHEMA_PATH}/bigquery_schema.json", 'w') as f:
        json.dump(schema, f, indent=2)
    
    return schema


dag = DAG(
    'us_accidents_exploration',
    default_args=default_args,
    description='Explore US Accidents dataset and prepare schema',
    schedule_interval=None,
    start_date=datetime(2025, 4, 10),
    catchup=False,
    tags=['us_accidents', 'exploration'],
)


sample_task = PythonOperator(
    task_id='sample_data',
    python_callable=sample_data,
    dag=dag,
)

explore_task = PythonOperator(
    task_id='explore_data',
    python_callable=explore_data,
    dag=dag,
)

schema_task = PythonOperator(
    task_id='generate_bigquery_schema',
    python_callable=generate_bigquery_schema,
    dag=dag,
)

sample_task >> explore_task >> schema_task