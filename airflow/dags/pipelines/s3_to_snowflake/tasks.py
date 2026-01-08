from pipelines.s3_to_snowflake.config import S3_BUCKET, S3_PREFIX
from pipelines.s3_to_snowflake.s3_service import S3Service
from pipelines.s3_to_snowflake.validations import validate_file_count


def count_s3_files_task(**context):
    """
    Airflow task callable.
    Counts CSV files in S3 and pushes expected count to XCom.
    """
    s3 = S3Service(S3_BUCKET, S3_PREFIX)
    count = s3.count_files()

    validate_file_count(count)

    context["ti"].xcom_push(
        key="expected_files",
        value=count
    )
