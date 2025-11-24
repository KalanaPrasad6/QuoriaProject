from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT", "quoriagigamartproject")
REGION = "asia-south1"

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="gigamart_hourly_pos_freshness",
    default_args=default_args,
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gigamart", "monitoring", "hourly"],
) as dag:

    freshness_check = BigQueryInsertJobOperator(
        task_id="freshness_check_pos",
        configuration={
            "query": {
                "query": f"""
                SELECT COUNT(*) AS recent_rows
                FROM `{PROJECT_ID}.giga_raw.raw_pos_transactions`
                WHERE ingestion_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    freshness_check
