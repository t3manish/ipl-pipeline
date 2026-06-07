# IPL Data Intelligence Pipeline

End-to-end data engineering pipeline processing IPL cricket data
using PySpark, AWS S3, Snowflake, dbt, and Apache Airflow.

## Architecture

CSV → Python/boto3 → S3 (raw) → PySpark → S3 (processed)
     → Snowflake (raw schema) → dbt (staging + mart) → Airflow DAG

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, boto3, AWS S3 |
| Processing | Apache PySpark |
| Warehouse | Snowflake |
| Transformation | dbt (staging → mart) |
| Orchestration | Apache Airflow |
| Environment | Docker, Jupyter |

## Pipeline Output

- `mart_team_performance` — team wins, win %, rank, performance tier
- `stg_season_summary` — matches per season with growth trend

## Setup

1. Clone repo
2. Add `.env` with AWS + Snowflake credentials
3. `docker-compose up -d`
4. Run notebooks 1–3 in order
5. Trigger Airflow DAG at localhost:8080

## Key Results

- Mumbai Indians: most wins across all seasons
- Toss winner wins match ~51% of the time
- IPL grew from 58 matches (2008) to 74 matches (2019)
