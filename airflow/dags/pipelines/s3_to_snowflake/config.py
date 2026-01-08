S3_BUCKET = "snowflake-data-engineer-project"
S3_PREFIX = "raw/revenue/year=2024/"

SNOWFLAKE_STAGE = "@BRONZE.PUBLIC.S3"
SNOWFLAKE_TABLE = "BRONZE.PUBLIC.bronze_revenue"
SNOWFLAKE_FILE_FORMAT = "BRONZE.PUBLIC.bronze_csv_format"
