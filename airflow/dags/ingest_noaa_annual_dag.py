from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from weather_pipeline.ingestion.ingest_noaa_annual_aws_s3 import run

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="ingest_noaa_annual",
    description="Annual incremental ingestion of NOAA weather data into Bronze layer",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@yearly",
    catchup=False,
    tags=["noaa", "weather", "bronze", "ingestion"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    ingest_noaa_annual = PythonOperator(
        task_id="ingest_noaa_annual",
        python_callable=run,
    )

    start >> ingest_noaa_annual >> end
