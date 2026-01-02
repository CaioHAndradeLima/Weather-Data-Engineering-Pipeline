# Weather Data Engineering Pipeline (ELT)

## Overview
This project is an end-to-end **Data Engineering ELT pipeline** built using real public data from the **NOAA Global Historical Climate Network (GHCN-Daily)** dataset hosted on the AWS Open Data Registry.

The goal of this project is to demonstrate how to design, orchestrate, transform, and model large-scale batch data pipelines using industry-standard tools and best practices commonly used by Data Engineers.

---

## Architecture Overview

- **Data Source**: NOAA GHCN-Daily (Public AWS S3 Bucket)
- **Orchestrator**: Apache Airflow
- **Storage**: Amazon S3 (Data Lake)
- **Transformations**: DBT (SQL-based ELT)
- **Architecture Pattern**: Medallion Architecture (Bronze / Silver / Gold)
- **Local Development**: Docker & Docker Compose

---

## Data Flow

1. **Ingestion (Bronze Layer)**
   - Apache Airflow orchestrates batch ingestion jobs.
   - Raw weather data files are copied from the public NOAA S3 bucket.
   - Data is stored *as-is* in the Bronze layer.

2. **Transformation (Silver Layer)**
   - Raw data is cleaned, validated, and standardized.
   - Data types are enforced and missing values handled.

3. **Analytics (Gold Layer)**
   - Aggregated and analytics-ready tables are created.
   - Data is optimized for reporting and downstream consumption.

---

## Data Layers

| Layer  | Description |
|------|------------|
| Bronze | Raw data exactly as received from the source |
| Silver | Cleaned, typed, and normalized datasets |
| Gold | Business-ready analytical tables |

---

## Project Structure

```text
weather-data-pipeline/
├── README.md
├── docker/
│   └── docker-compose.yml
├── airflow/
│   ├── dags/
│   └── plugins/
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
├── scripts/
│   └── ingest_noaa.py
└── docs/
    └── architecture.png
```

---

## Tools & Technologies

- Python
- Apache Airflow
- Amazon S3
- DBT
- SQL
- Docker

---

## Data Source

- **Dataset**: NOAA Global Historical Climate Network – Daily (GHCN-Daily)
- **Provider**: NOAA
- **AWS Open Data Registry**
- **Public S3 Bucket**: `s3://noaa-ghcn-pds/`

---

## Project Goals

- Build a realistic batch ELT pipeline using real-world public data
- Apply medallion architecture best practices
- Demonstrate orchestration vs transformation responsibilities
- Create a strong, production-like Data Engineering portfolio project

---

## Future Improvements

- Incremental ingestion strategy
- Apache Spark transformations
- Data quality checks with Soda
- Metadata management with OpenMetadata
- Streaming ingestion with Kafka
- Cloud deployment on AWS (IAM, Athena, Redshift)

---

## Getting Started

### Prerequisites
- Docker & Docker Compose
- AWS CLI
- Python 3.10+

### Validate Data Access
```bash
aws s3 ls s3://noaa-ghcn-pds/ --no-sign-request
```

---

## License
This project is for educational and portfolio purposes.
