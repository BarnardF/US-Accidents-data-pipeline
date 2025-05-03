from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Column name mapping to sanitize field names
COLUMN_NAME_MAPPING = {
    'Distance(mi)': 'Distance_mi',
    'Temperature(F)': 'Temperature_F',
    'Wind_Chill(F)': 'Wind_Chill_F',
    'Humidity(%)': 'Humidity_Pct',
    'Pressure(in)': 'Pressure_in',
    'Visibility(mi)': 'Visibility_mi',
    'Wind_Speed(mph)': 'Wind_Speed_mph',
    'Precipitation(in)': 'Precipitation_in'
}

# Paths
RAW_DATA_PATH = '/opt/airflow/data/raw/US_Accidents_March23.csv'
CHUNKS_DIR = '/opt/airflow/data/processed/chunks'

def chunk_csv_by_month():
    os.makedirs(CHUNKS_DIR, exist_ok=True)
    chunk_size = 50_000
    chunk_counter = 0

    print("Reading and processing in chunks...")
    for chunk in pd.read_csv(RAW_DATA_PATH, chunksize=chunk_size):
        chunk_counter += 1
        print(f"Processing chunk #{chunk_counter}")

        chunk.columns = chunk.columns.str.strip()
        chunk.rename(columns=COLUMN_NAME_MAPPING, inplace=True)

        chunk['Start_Time'] = pd.to_datetime(chunk['Start_Time'], errors='coerce')
        chunk.dropna(subset=['Start_Time'], inplace=True)
        chunk['YearMonth'] = chunk['Start_Time'].dt.to_period('M')

        for year_month, group in chunk.groupby('YearMonth'):
            output_path = os.path.join(CHUNKS_DIR, f"us_accidents_{year_month}.csv")
            if os.path.exists(output_path):
                group.drop(columns=['YearMonth']).to_csv(output_path, mode='a', index=False, header=False)
            else:
                group.drop(columns=['YearMonth']).to_csv(output_path, index=False)
            print(f"Saved: {output_path}")

    print("All chunks processed and saved.")

with DAG(
    dag_id='us_accidents_chunk_csv',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    chunk_task = PythonOperator(
        task_id='chunk_csv_by_month',
        python_callable=chunk_csv_by_month
    )
