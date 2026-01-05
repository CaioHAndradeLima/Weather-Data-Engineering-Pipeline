from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from datetime import datetime


def run():
    print("Post-ingestion message/logic")


with DAG(
    dag_id="ingest_bronze_postgres_snowflake_categories",
    description="Ingest categories table data from Postgres to Snowflake (Bronze)",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0,30 * * * *",
    catchup=False,
    tags=["bronze", "ingestion", "airbyte"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Airbyte ingestion group
    with TaskGroup(group_id="airbyte_ingestion") as airbyte_ingestion:
        trigger_sync = AirbyteTriggerSyncOperator(
            task_id="trigger_airbyte_sync",
            airbyte_conn_id="airbyte_local",
            connection_id="9520e86d-10a7-44e7-89d7-2aa7d37f2777",
            asynchronous=True,
        )

        monitor_sync = AirbyteJobSensor(
            task_id="monitor_airbyte_sync",
            airbyte_conn_id="airbyte_local",
            airbyte_job_id=trigger_sync.output,
        )

        trigger_sync >> monitor_sync

    # Optional post-ingestion step
    post_ingestion = PythonOperator(
        task_id="post_ingestion_validation",
        python_callable=run,
    )

    end = EmptyOperator(task_id="end")

    # DAG orchestration
    start >> airbyte_ingestion >> post_ingestion >> end
