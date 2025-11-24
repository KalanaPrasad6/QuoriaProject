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
RAW_BUCKET = "giga-raw-quoriagigamartproject"

FLEX_TEMPLATE_ECOM = f"gs://{TEMP_BUCKET}/flex-templates/ecommerce_batch.json"

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email": EMAIL_ON_FAILURE,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
}

with DAG(
    dag_id="gigamart_hourly_ecommerce",
    default_args=default_args,
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gigamart", "ecommerce", "hourly"],
) as dag:

    ecom_batch = DataflowStartFlexTemplateOperator(
        task_id="ecom_batch_hourly",
        project_id=PROJECT_ID,
        location=REGION,
        body={
            "launchParameter": {
                "jobName": "giga-ecom-hourly-{{ ds_nodash }}-{{ ts_nodash | lower }}",
                "containerSpecGcsPath": FLEX_TEMPLATE_ECOM,
                "environment": {
                    "tempLocation": f"gs://{TEMP_BUCKET}/temp",
                    "stagingLocation": f"gs://{STAGING_BUCKET}/staging",
                    "serviceAccountEmail": f"{PROJECT_ID}-compute@developer.gserviceaccount.com",
                    "maxWorkers": 10,
                    "numWorkers": 2,
                },
                "parameters": {
                    "input_pattern": f"gs://{RAW_BUCKET}/ecom-orders/hourly/{{{{ ds }}}}/{{{{ execution_date.hour }}}}/*.json",
                    "output_table": f"{PROJECT_ID}:giga_raw.raw_ecom_orders",
                },
            }
        },
    )

    deduplication = BigQueryInsertJobOperator(
        task_id="deduplicate_ecom_orders",
        configuration={
            "query": {
                "query": "CALL `giga_raw.sp_deduplicate_ecom_orders`()",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    monitor_execution = BigQueryInsertJobOperator(
        task_id="log_execution_metrics",
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `{PROJECT_ID}.giga_curated.pipeline_monitoring` (
                    pipeline_name, run_date, run_timestamp, status,
                    records_processed, records_failed, execution_time_seconds
                )
                SELECT
                    'gigamart_hourly_ecommerce',
                    CURRENT_DATE(),
                    CURRENT_TIMESTAMP(),
                    'SUCCESS',
                    COUNT(*) AS records_processed,
                    0 AS records_failed,
                    0 AS execution_time_seconds
                FROM `{PROJECT_ID}.giga_raw.raw_ecom_orders`
                WHERE ingestion_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    ecom_batch >> deduplication >> monitor_execution
