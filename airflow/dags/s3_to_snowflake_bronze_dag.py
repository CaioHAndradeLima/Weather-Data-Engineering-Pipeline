from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from airflow.dags.pipelines.s3_to_snowflake.config import (
    S3_BUCKET,
    S3_PREFIX,
    SNOWFLAKE_STAGE,
    SNOWFLAKE_TABLE,
    SNOWFLAKE_FILE_FORMAT,
)
from airflow.dags.pipelines.s3_to_snowflake.snowflake_service import SnowflakeCopyBuilder
from airflow.dags.pipelines.s3_to_snowflake.tasks import count_s3_files_task


with DAG(
    dag_id="s3_to_snowflake_bronze",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bronze", "s3", "snowflake"],
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup("s3_tasks") as s3_tasks:
        wait_for_files = S3KeySensor(
            task_id="wait_for_files",
            bucket_name=S3_BUCKET,
            bucket_key=f"{S3_PREFIX}*.csv",
            wildcard_match=True,
        )

        count_files = PythonOperator(
            task_id="count_files",
            python_callable=count_s3_files_task,
        )

        wait_for_files >> count_files

    copy_builder = SnowflakeCopyBuilder(
        stage=SNOWFLAKE_STAGE,
        table=SNOWFLAKE_TABLE,
        file_format=SNOWFLAKE_FILE_FORMAT,
        prefix=S3_PREFIX,
    )

    load_to_bronze = SnowflakeOperator(
        task_id="copy_into_bronze",
        snowflake_conn_id="snowflake_default",
        sql=copy_builder.copy_into_sql(),
    )

    end = EmptyOperator(task_id="end")

    start >> s3_tasks >> load_to_bronze >> end
