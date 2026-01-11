from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import your script
from create_connection import run

with DAG(
    dag_id="airbyte_manage_connections_v1",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["airbyte", "infra", "v1"],
) as dag:

    manage_connections = PythonOperator(
        task_id="create_or_update_connections",
        python_callable=run,
    )

    manage_connections
