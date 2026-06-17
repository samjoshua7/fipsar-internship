from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    "owner": "joshua",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="api_data_ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
) as dag:

    run_ingestion = BashOperator(
        task_id="run_ingestion_script",
        bash_command="""
        cd /opt/airflow/project &&
        python scripts/ingest_posts.py
        """
    )