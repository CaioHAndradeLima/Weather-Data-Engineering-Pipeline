from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from datetime import datetime

with DAG(
    dag_id="airbyte_connection_smoke_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    trigger = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id="airbyte_local",
        connection_id="9520e86d-10a7-44e7-89d7-2aa7d37f2777",
        asynchronous=False,
        timeout=600,
    )
