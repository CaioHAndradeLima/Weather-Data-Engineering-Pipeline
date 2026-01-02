from weather_pipeline.ingestion.ingest_noaa_annual_aws_s3 import START_YEAR


def test_start_year_is_not_too_old():
    assert START_YEAR >= 2020
