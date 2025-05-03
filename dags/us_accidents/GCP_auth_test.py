from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 11)
}

with DAG(
    'gcp_auth_test',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['test']
) as dag:

    test_query = BigQueryExecuteQueryOperator(
        task_id='test_query',
        sql='SELECT 1',
        use_legacy_sql=False,
        location='US',
    )
