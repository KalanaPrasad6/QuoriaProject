from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.email import EmailOperator

PROJECT_ID = os.environ.get("GCP_PROJECT", "quoriagigamartproject")
REGION = os.environ.get("GCP_REGION", "asia-south1")

EMAIL_ON_FAILURE = os.environ.get("EMAIL_ON_FAILURE", "data-eng@example.com").split(",")

RAW_BUCKET = os.environ.get("RAW_BUCKET", "giga-raw-quoriagigamartproject")
STAGING_BUCKET = os.environ.get("STAGING_BUCKET", "giga-dataflow-staging-quoriagigamartproject")
TEMP_BUCKET = os.environ.get("TEMP_BUCKET", "giga-dataflow-temp-quoriagigamartproject")

# Dataflow worker service account
DATAFLOW_WORKER_SA = os.environ.get("DATAFLOW_WORKER_SA", "")

BASE_ENVIRONMENT = {
    "tempLocation": f"gs://{TEMP_BUCKET}/temp",
    "stagingLocation": f"gs://{STAGING_BUCKET}/staging",
}
if DATAFLOW_WORKER_SA:
    BASE_ENVIRONMENT["serviceAccountEmail"] = DATAFLOW_WORKER_SA

# Flex template locations
FLEX_TEMPLATE_ECOM = f"gs://{TEMP_BUCKET}/flex-templates/ecommerce_batch.json"
FLEX_TEMPLATE_CRM = f"gs://{TEMP_BUCKET}/flex-templates/crm_batch.json"
FLEX_TEMPLATE_INVENTORY = f"gs://{TEMP_BUCKET}/flex-templates/inventory_batch.json"
FLEX_TEMPLATE_POS = f"gs://{TEMP_BUCKET}/flex-templates/pos_batch.json"

# DAG default args
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email": EMAIL_ON_FAILURE,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

# DAG definition
with DAG(
    dag_id="gigamart_daily_batch",
    default_args=default_args,
    schedule_interval="0 0 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gigamart", "batch"],
) as dag:

    # 1) Initialize BigQuery datasets, tables, procedures

    bq_init_all = BigQueryInsertJobOperator(
        task_id="bq_init_all",
        configuration={
            "query": {
                "query": (
                    open("/home/airflow/gcs/dags/sql/00_create_datasets.sql").read()
                    + "\n" + open("/home/airflow/gcs/dags/sql/01_create_raw_tables.sql").read()
                    + "\n" + open("/home/airflow/gcs/dags/sql/02_create_curated_tables.sql").read()
                    + "\n" + open("/home/airflow/gcs/dags/sql/03_sp_customer_dim_scd2.sql").read()
                    + "\n" + open("/home/airflow/gcs/dags/sql/04_optimize_tables.sql").read()
                    + "\n" + open("/home/airflow/gcs/dags/sql/05_deduplication_procedure.sql").read()
                    + "\n" + open("/home/airflow/gcs/dags/sql/07_late_arriving_data.sql").read()
                    + "\n" + open("/home/airflow/gcs/dags/sql/08_monitoring_queries.sql").read()
                ),
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    # 2) Dataflow Flex template batch jobs

    dataflow_common_kwargs = {
        "project_id": PROJECT_ID,
        "location": REGION,
        "gcp_conn_id": "google_cloud_default",
        "wait_until_finished": False,
    }

    ecom_batch = DataflowStartFlexTemplateOperator(
        task_id="ecom_batch",
        body={
            "launchParameter": {
                "jobName": "giga-ecom-batch-{{ ds_nodash }}",
                "containerSpecGcsPath": FLEX_TEMPLATE_ECOM,
                "environment": dict(BASE_ENVIRONMENT),
                "parameters": {
                    "input_pattern": f"gs://{RAW_BUCKET}/ecom-orders/*",
                    "output_table": f"{PROJECT_ID}:giga_raw.raw_ecom_orders",
                },
            }
        },
        **dataflow_common_kwargs,
    )

    crm_batch = DataflowStartFlexTemplateOperator(
        task_id="crm_batch",
        body={
            "launchParameter": {
                "jobName": "giga-crm-batch-{{ ds_nodash }}",
                "containerSpecGcsPath": FLEX_TEMPLATE_CRM,
                "environment": dict(BASE_ENVIRONMENT),
                "parameters": {
                    "input_pattern": f"gs://{RAW_BUCKET}/crm-profiles/*.csv",
                    "output_table": f"{PROJECT_ID}:giga_raw.raw_crm_customers",
                },
            }
        },
        **dataflow_common_kwargs,
    )

    inventory_batch = DataflowStartFlexTemplateOperator(
        task_id="inventory_batch",
        body={
            "launchParameter": {
                "jobName": "giga-inventory-batch-{{ ds_nodash }}",
                "containerSpecGcsPath": FLEX_TEMPLATE_INVENTORY,
                "environment": dict(BASE_ENVIRONMENT),
                "parameters": {
                    "input_pattern": f"gs://{RAW_BUCKET}/inventory/*.csv",
                    "output_table": f"{PROJECT_ID}:giga_raw.raw_inventory_snapshots",
                },
            }
        },
        **dataflow_common_kwargs,
    )

    pos_batch = DataflowStartFlexTemplateOperator(
        task_id="pos_batch",
        body={
            "launchParameter": {
                "jobName": "giga-pos-batch-{{ ds_nodash }}",
                "containerSpecGcsPath": FLEX_TEMPLATE_POS,
                "environment": dict(BASE_ENVIRONMENT),
                "parameters": {
                    "input_pattern": f"gs://{RAW_BUCKET}/pos/*.json",
                    "output_table": f"{PROJECT_ID}:giga_raw.raw_pos_transactions",
                },
            }
        },
        **dataflow_common_kwargs,
    )

    # 3) BigQuery SCD2, deduplication, late data, DQ, monitoring

    scd2_customer_dim = BigQueryInsertJobOperator(
        task_id="scd2_customer_dim",
        configuration={
            "query": {
                "query": "CALL `giga_curated.sp_upsert_customer_dim_scd2`()",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    deduplicate_pos = BigQueryInsertJobOperator(
        task_id="deduplicate_pos",
        configuration={
            "query": {
                "query": "CALL `giga_raw.sp_deduplicate_pos_transactions`()",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    deduplicate_crm = BigQueryInsertJobOperator(
        task_id="deduplicate_crm",
        configuration={
            "query": {
                "query": "CALL `giga_raw.sp_deduplicate_crm_customers`()",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    deduplicate_ecom = BigQueryInsertJobOperator(
        task_id="deduplicate_ecom",
        configuration={
            "query": {
                "query": "CALL `giga_raw.sp_deduplicate_ecom_orders`()",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    process_late_data = BigQueryInsertJobOperator(
        task_id="process_late_arriving_data",
        configuration={
            "query": {
                "query": "CALL `giga_curated.sp_process_late_arriving_data`('all', 7)",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    dq_checks = BigQueryInsertJobOperator(
        task_id="dq_checks",
        configuration={
            "query": {
                "query": open("/home/airflow/gcs/dags/sql/06_dq_checks.sql").read(),
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    check_pipeline_health = BigQueryInsertJobOperator(
        task_id="check_pipeline_health",
        configuration={
            "query": {
                "query": f"""
                SELECT
                    pipeline_name,
                    COUNT(*) AS failed_runs,
                    MAX(run_timestamp) AS last_failure
                FROM `{PROJECT_ID}.giga_curated.pipeline_monitoring`
                WHERE status = 'FAILED'
                  AND run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                GROUP BY pipeline_name
                HAVING COUNT(*) > 0;
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    # 4) Explicit email notification on any failure

    send_failure_email = EmailOperator(
        task_id="send_failure_notification",
        to=EMAIL_ON_FAILURE,
        subject="GigaMart Daily Batch Pipeline Failure",
        html_content=f"""
            <h2>Pipeline Failure Alert</h2>
            <p>The GigaMart daily batch pipeline has failed.</p>
            <p>Please check the Airflow logs for details.</p>
            <p>Project: {PROJECT_ID}</p>
            <p>Execution Date: {{ '{{ ds }}' }}</p>
            """,
        trigger_rule="one_failed",
    )

    # Dependencies
    # 1) Raw loads run independently
    # 2) Deduplicate after loads

    pos_batch >> deduplicate_pos
    crm_batch >> deduplicate_crm
    ecom_batch >> deduplicate_ecom

    # 3) SCD2 + late data must wait for:
    #    - all deduped raw tables
    #    - inventory load
    #    - and BigQuery init (procedures, tables)

    [deduplicate_pos, deduplicate_crm, deduplicate_ecom, inventory_batch, bq_init_all] >> scd2_customer_dim
    scd2_customer_dim >> process_late_data

    # 4) DQ + monitoring

    process_late_data >> dq_checks >> check_pipeline_health

    # 5) Failure notification if ANY upstream step fails
    [
        ecom_batch,
        crm_batch,
        inventory_batch,
        pos_batch,
        deduplicate_pos,
        deduplicate_crm,
        deduplicate_ecom,
        scd2_customer_dim,
        dq_checks,
        process_late_data,
        bq_init_all,
    ] >> send_failure_email