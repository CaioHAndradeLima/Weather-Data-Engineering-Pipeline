import boto3
from botocore.exceptions import ClientError

# ==============================
# Configuration
# ==============================
SOURCE_BUCKET = "noaa-ghcn-pds"
SOURCE_PREFIX = "parquet/by_year"
DEST_BUCKET = "weather-data-lake-caio"
DEST_PREFIX = "bronze/noaa/observations"

START_YEAR = 2024

s3 = boto3.client("s3")


# ==============================
# Helpers
# ==============================
def list_source_years():
    """
    List available years in the NOAA public S3 bucket.
    """
    paginator = s3.get_paginator("list_objects_v2")
    result = set()

    for page in paginator.paginate(
        Bucket=SOURCE_BUCKET,
        Prefix=f"{SOURCE_PREFIX}/",
        RequestPayer="requester",
    ):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            if len(parts) >= 3 and parts[2].isdigit():
                result.add(int(parts[2]))

    return sorted(result)


def year_already_ingested(year: int) -> bool:
    """
    Check if a given year already exists in the Bronze layer.
    """
    try:
        s3.head_object(
            Bucket=DEST_BUCKET,
            Key=f"{DEST_PREFIX}/year={year}/",
        )
        return True
    except ClientError:
        return False


def copy_year(year: int):
    """
    Copy one year of NOAA data from the public bucket to Bronze.
    """
    source = f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}/{year}/"
    destination = f"s3://{DEST_BUCKET}/{DEST_PREFIX}/year={year}/"

    print(f"📥 Copying data for year {year}")
    print(f"    Source:      {source}")
    print(f"    Destination: {destination}")

    boto3.resource("s3").meta.client.copy(
        {
            "Bucket": SOURCE_BUCKET,
            "Key": f"{SOURCE_PREFIX}/{year}/",
        },
        DEST_BUCKET,
        f"{DEST_PREFIX}/year={year}/",
    )


# ==============================
# Main
# ==============================
def run():
    print("🚀 Starting annual NOAA ingestion")
    print(f"📅 Start year: {START_YEAR}")

    available_years = list_source_years()
    years_to_process = [y for y in available_years if y >= START_YEAR]

    print(f"📊 Available years from NOAA: {years_to_process}")

    for year in years_to_process:
        if year_already_ingested(year):
            print(f"⏭️  Year {year} already ingested. Skipping.")
            continue

        print(f"✅ Ingesting year {year}")
        print(
            f"aws s3 sync "
            f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}/{year}/ "
            f"s3://{DEST_BUCKET}/{DEST_PREFIX}/year={year}/ "
            f"--no-sign-request"
        )

        # Use sync logic via subprocess (safe & efficient)
        import subprocess

        subprocess.run(
            [
                "aws",
                "s3",
                "sync",
                f"s3://{SOURCE_BUCKET}/{SOURCE_PREFIX}/{year}/",
                f"s3://{DEST_BUCKET}/{DEST_PREFIX}/year={year}/",
                "--no-sign-request",
            ],
            check=True,
        )

    print("🎉 Annual NOAA ingestion completed successfully")


if __name__ == "__main__":
    run()
