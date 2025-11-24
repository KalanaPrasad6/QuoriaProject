from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.email import EmailOperator

PROJECT_ID = os.environ.get("GCP_PROJECT", "quoriagigamartproject")
REGION = "asia-south1"
EMAIL_ON_FAILURE = os.environ.get("EMAIL_ON_FAILURE", "data-eng@example.com").split(",")

TEMP_BUCKET = "giga-dataflow-temp-quoriagigamartproject"
STAGING_BUCKET = "giga-dataflow-staging-quoriagigamartproject"
PUBSUB_TOPIC = f"projects/{PROJECT_ID}/topics/pos-transactions"
PUBSUB_SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/pos-transactions-sub"

FLEX_TEMPLATE_POS_STREAMING = f"gs://{TEMP_BUCKET}/flex-templates/pos_streaming.json"

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email": EMAIL_ON_FAILURE,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "max_active_runs": 1,
}

with DAG(
    dag_id="gigamart_realtime_pos",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gigamart", "pos", "streaming", "realtime"],
) as dag:

    # Start the streaming job
    start_pos_streaming = DataflowStartFlexTemplateOperator(
        task_id="start_pos_streaming",
        project_id=PROJECT_ID,
        location=REGION,
        body={
            "launchParameter": {
                "jobName": "giga-pos-streaming",
                "containerSpecGcsPath": FLEX_TEMPLATE_POS_STREAMING,
                "environment": {
                    "tempLocation": f"gs://{TEMP_BUCKET}/temp",
                    "stagingLocation": f"gs://{STAGING_BUCKET}/staging",
                    "serviceAccountEmail": f"{PROJECT_ID}-compute@developer.gserviceaccount.com",
                    "maxWorkers": 5,
                    "numWorkers": 2,
                },
                "parameters": {
                    "input_subscription": PUBSUB_SUBSCRIPTION,
                    "output_table": f"{PROJECT_ID}:giga_raw.raw_pos_transactions",
                    "window_size": "60",
                    "allowed_lateness": "3600",
                },
            }
        },
        wait_until_finished=False,
    )

    # Monitor whether streaming job is still ingesting data
    pos_streaming_monitor = BigQueryInsertJobOperator(
        task_id="monitor_streaming_job",
        configuration={
            "query": {
                "query": f"""
                SELECT
                    COUNT(*) AS recent_transactions,
                    MAX(ingestion_ts) AS latest_ingestion
                FROM `{PROJECT_ID}.giga_raw.raw_pos_transactions`
                WHERE ingestion_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE);
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    # Deduplication for streaming data
    deduplication = BigQueryInsertJobOperator(
        task_id="deduplicate_pos_streaming",
        configuration={
            "query": {
                "query": "CALL `giga_raw.sp_deduplicate_pos_transactions`()",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    start_pos_streaming >> pos_streaming_monitor >> deduplication