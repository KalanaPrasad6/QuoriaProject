from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryValueCheckOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator

PROJECT_ID = os.environ.get("GCP_PROJECT", "quoriagigamartproject")
REGION = "asia-south1"
EMAIL_ALERTS = os.environ.get("EMAIL_ALERTS", "data-eng@example.com").split(",")

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email": EMAIL_ALERTS,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def check_pipeline_health(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    SELECT
        pipeline_name,
        COUNT(*) AS failed_runs,
        MAX(run_timestamp) AS last_failure,
        MAX(error_message) AS latest_error
    FROM `{PROJECT_ID}.giga_curated.pipeline_monitoring`
    WHERE status = 'FAILED'
        AND run_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    GROUP BY pipeline_name
    """
    
    results = client.query(query).result()
    failures = list(results)
    
    if failures:
        failure_details = "\n".join([
            f"- {row.pipeline_name}: {row.failed_runs} failures, Last: {row.last_failure}"
            for row in failures
        ])
        print(f"ALERT: Pipeline failures detected:\n{failure_details}")
    
    return len(failures) == 0

def check_data_freshness(**context):
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    SELECT
        'raw_pos_transactions' AS table_name,
        MAX(ingestion_ts) AS latest_ingestion,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingestion_ts), MINUTE) AS minutes_since_last_ingestion
    FROM `{PROJECT_ID}.giga_raw.raw_pos_transactions`
    
    UNION ALL
    
    SELECT
        'raw_ecom_orders' AS table_name,
        MAX(ingestion_ts) AS latest_ingestion,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingestion_ts), MINUTE) AS minutes_since_last_ingestion
    FROM `{PROJECT_ID}.giga_raw.raw_ecom_orders`
    """
    
    results = client.query(query).result()
    stale_tables = []
    
    for row in results:
        if row.minutes_since_last_ingestion and row.minutes_since_last_ingestion > 120:
            stale_tables.append(f"{row.table_name}: {row.minutes_since_last_ingestion} minutes")
    
    if stale_tables:
        print(f"ALERT: Stale data detected:\n" + "\n".join(stale_tables))
    
    return len(stale_tables) == 0

with DAG(
    dag_id="gigamart_monitoring",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gigamart", "monitoring", "alerting"],
) as dag:

    check_pipeline_failures = PythonOperator(
        task_id="check_pipeline_failures",
        python_callable=check_pipeline_health,
    )

    check_data_freshness_task = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    check_duplicate_rate = BigQueryValueCheckOperator(
        task_id="check_duplicate_rate",
        sql=f"""
        SELECT
            COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_count
        FROM `{PROJECT_ID}.giga_raw.raw_pos_transactions`
        WHERE ingestion_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """,
        pass_value=100,
        tolerance=0.1,
        location=REGION,
    )

    check_error_rate = BigQueryValueCheckOperator(
        task_id="check_error_rate",
        sql=f"""
        SELECT
            SUM(records_failed) AS total_failures
        FROM `{PROJECT_ID}.giga_curated.pipeline_monitoring`
        WHERE run_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """,
        pass_value=1000,
        tolerance=0.1,
        location=REGION,
    )

    generate_report = BigQueryInsertJobOperator(
        task_id="generate_monitoring_report",
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `{PROJECT_ID}.giga_curated.pipeline_monitoring` (
                    pipeline_name, run_date, run_timestamp, status,
                    records_processed, records_failed, execution_time_seconds, metadata
                )
                SELECT
                    'gigamart_monitoring',
                    CURRENT_DATE(),
                    CURRENT_TIMESTAMP(),
                    'SUCCESS',
                    0,
                    0,
                    0,
                    JSON_OBJECT(
                        'pipeline_failures', (
                            SELECT COUNT(*)
                            FROM `{PROJECT_ID}.giga_curated.pipeline_monitoring`
                            WHERE status = 'FAILED'
                                AND run_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
                        ),
                        'stale_tables', (
                            SELECT COUNT(*)
                            FROM (
                                SELECT MAX(ingestion_ts) AS latest
                                FROM `{PROJECT_ID}.giga_raw.raw_pos_transactions`
                                HAVING TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), latest, MINUTE) > 120
                            )
                        )
                    )
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    [check_pipeline_failures, check_data_freshness_task, check_duplicate_rate, check_error_rate] >> generate_report
