from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner"           : "manish",
    "retries"         : 2,
    "retry_delay"     : timedelta(minutes=2),
    "email_on_failure": False,
}

# ── TASK FUNCTIONS ────────────────────────────────────

def task_ingest():
    """Task 1: Download IPL CSV → upload raw Parquet to S3"""
    import urllib.request
    import boto3
    import os
    from pathlib import Path

    print("[INGEST] Starting...")

    # Download CSV
    Path("data").mkdir(exist_ok=True)
    url = "https://raw.githubusercontent.com/dsrscientist/dataset1/master/IPL%20matches%202008-2020.csv"
    urllib.request.urlretrieve(url, "data/ipl_matches.csv")
    print("  ✅ CSV downloaded")

    # Upload to S3
    s3 = boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name           = os.getenv("AWS_REGION", "ap-south-1")
    )
    s3.upload_file("data/ipl_matches.csv",
                   os.getenv("BUCKET_NAME"),
                   "raw/ipl_matches.csv")
    print("  ✅ CSV uploaded to S3")


def task_transform():
    """Task 2: PySpark — read S3 raw → transform → write S3 processed"""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import count, col, desc
    from pathlib import Path

    print("[TRANSFORM] Starting PySpark...")

    spark = SparkSession.builder \
        .appName("IPL-Transform") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.csv("data/ipl_matches.csv", header=True, inferSchema=True)
    print(f"  Rows: {df.count()}")

    team_wins = df.filter(col("winner").isNotNull()) \
        .groupBy("winner").agg(count("*").alias("total_wins")) \
        .orderBy(desc("total_wins"))

    season_summary = df.groupBy("season") \
        .agg(count("*").alias("matches_played")).orderBy("season")

    toss_analysis = df.filter(col("winner").isNotNull()) \
        .groupBy("toss_decision").agg(count("*").alias("total_matches")) \
        .orderBy(desc("total_matches"))

    Path("output").mkdir(exist_ok=True)
    team_wins.write.mode("overwrite").parquet("output/team_wins")
    season_summary.write.mode("overwrite").parquet("output/season_summary")
    toss_analysis.write.mode("overwrite").parquet("output/toss_analysis")

    spark.stop()
    print("  ✅ Parquet files written")


def task_snowflake_load():
    """Task 3: Load Parquet → Snowflake"""
    import snowflake.connector
    import pandas as pd
    import os

    print("[SNOWFLAKE] Loading...")

    # Using the Key Pair Auth we set up earlier
    conn = snowflake.connector.connect(
        account          = os.getenv("SNOWFLAKE_ACCOUNT"),
        user             = os.getenv("SNOWFLAKE_USER"),
        private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "/opt/airflow/project/rsa_key.p8"),
        role             = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        database         = os.getenv("SNOWFLAKE_DATABASE", "IPL_DB"),
        schema           = os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        warehouse        = os.getenv("SNOWFLAKE_WAREHOUSE", "IPL_WH")
    )
    cur = conn.cursor()

    tables = {
        "IPL_DB.RAW.TEAM_WINS": (
            "output/team_wins",
            "CREATE OR REPLACE TABLE IPL_DB.RAW.TEAM_WINS (winner VARCHAR, total_wins INTEGER)"
        ),
        "IPL_DB.RAW.SEASON_SUMMARY": (
            "output/season_summary",
            "CREATE OR REPLACE TABLE IPL_DB.RAW.SEASON_SUMMARY (season INTEGER, matches_played INTEGER)"
        ),
        "IPL_DB.RAW.TOSS_ANALYSIS": (
            "output/toss_analysis",
            "CREATE OR REPLACE TABLE IPL_DB.RAW.TOSS_ANALYSIS (toss_decision VARCHAR, total_matches INTEGER)"
        ),
    }

    for table, (parquet_path, ddl) in tables.items():
        df = pd.read_parquet(parquet_path)
        cur.execute(ddl)
        cur.execute(f"TRUNCATE TABLE {table}")
        cols = ", ".join(df.columns)
        ph   = ", ".join(["%s"] * len(df.columns))
        rows = [tuple(r) for r in df.itertuples(index=False)]
        cur.executemany(f"INSERT INTO {table} ({cols}) VALUES ({ph})", rows)
        print(f"  ✅ {table}: {len(rows)} rows")

    cur.close()
    conn.close()
    print("  ✅ Snowflake load complete")


def task_dbt_run():
    """Task 4: Run dbt models + tests"""
    import subprocess
    import os

    dbt_project = "/opt/airflow/project/ipl_dbt"
    env = {**os.environ, "DBT_PROFILES_DIR": dbt_project}

    print("[DBT] Running models...")
    result = subprocess.run(
        ["dbt", "run", "--project-dir", dbt_project,
         "--profiles-dir", dbt_project],
        capture_output=True, text=True, env=env
    )
    print(result.stdout[-2000:])
    if result.returncode != 0:
        raise Exception(f"dbt run failed:\n{result.stderr}")

    print("[DBT] Running tests...")
    result = subprocess.run(
        ["dbt", "test", "--project-dir", dbt_project,
         "--profiles-dir", dbt_project],
        capture_output=True, text=True, env=env
    )
    print(result.stdout[-1000:])
    if result.returncode != 0:
        raise Exception(f"dbt test failed:\n{result.stderr}")

    print("  ✅ dbt run + test complete")


# ── DAG DEFINITION ────────────────────────────────────

with DAG(
    dag_id          = "ipl_pipeline",
    description     = "IPL end-to-end: ingest → PySpark → Snowflake → dbt",
    start_date      = datetime(2026, 6, 1),
    schedule_interval = "@daily",
    catchup         = False,
    default_args    = default_args,
    tags            = ["ipl", "pyspark", "snowflake", "dbt"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_to_s3",
        python_callable=task_ingest
    )

    transform = PythonOperator(
        task_id="spark_transform",
        python_callable=task_transform
    )

    load_sf = PythonOperator(
        task_id="snowflake_load",
        python_callable=task_snowflake_load
    )

    dbt = PythonOperator(
        task_id="dbt_run",
        python_callable=task_dbt_run
    )

    # Pipeline order
    ingest >> transform >> load_sf >> dbt
