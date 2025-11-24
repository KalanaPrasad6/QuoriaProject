-- Monitoring queries for pipeline health and performance

CREATE OR REPLACE VIEW `giga_curated.v_pipeline_execution_summary` AS
SELECT
  pipeline_name,
  run_date,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
  SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS failed_runs,
  AVG(records_processed)      AS avg_records_processed,
  AVG(execution_time_seconds) AS avg_execution_time_seconds,
  MAX(execution_time_seconds) AS max_execution_time_seconds
FROM `giga_curated.pipeline_monitoring`
WHERE run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY pipeline_name, run_date
ORDER BY run_date DESC, pipeline_name;

-- Data quality metrics (dq_status / dq_errors)

CREATE OR REPLACE VIEW `giga_curated.v_data_quality_metrics` AS
SELECT
  'raw_pos_transactions' AS table_name,
  COUNT(*) AS total_records,
  COUNT(DISTINCT transaction_id) AS unique_entities,
  COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_entity_count,
  COUNTIF(transaction_id IS NULL) AS null_entity_ids,
  COUNTIF(amount < 0) AS negative_amounts,
  COUNTIF(dq_status = 'INVALID') AS dq_invalid_rows,
  COUNTIF(dq_status IS NULL)     AS dq_status_null_rows,
  AVG(TIMESTAMP_DIFF(ingestion_ts, event_ts, HOUR)) AS avg_arrival_delay_hours
FROM `giga_raw.raw_pos_transactions`
WHERE ingestion_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)

UNION ALL

SELECT
  'raw_ecom_orders' AS table_name,
  COUNT(*) AS total_records,
  COUNT(DISTINCT order_id) AS unique_entities,
  COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_entity_count,
  COUNTIF(order_id IS NULL) AS null_entity_ids,
  COUNTIF(total_amount < 0) AS negative_amounts,
  COUNTIF(dq_status = 'INVALID') AS dq_invalid_rows,
  COUNTIF(dq_status IS NULL)     AS dq_status_null_rows,
  AVG(TIMESTAMP_DIFF(ingestion_ts, order_ts, HOUR)) AS avg_arrival_delay_hours
FROM `giga_raw.raw_ecom_orders`
WHERE ingestion_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)

UNION ALL

SELECT
  'raw_crm_customers' AS table_name,
  COUNT(*) AS total_records,
  COUNT(DISTINCT customer_id) AS unique_entities,
  COUNT(*) - COUNT(DISTINCT customer_id) AS duplicate_entity_count,
  COUNTIF(customer_id IS NULL) AS null_entity_ids,
  0 AS negative_amounts,
  COUNTIF(dq_status = 'INVALID') AS dq_invalid_rows,
  COUNTIF(dq_status IS NULL)     AS dq_status_null_rows,
  AVG(TIMESTAMP_DIFF(ingestion_ts, updated_at, HOUR)) AS avg_arrival_delay_hours
FROM `giga_raw.raw_crm_customers`
WHERE ingestion_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)

UNION ALL

SELECT
  'raw_inventory_snapshots' AS table_name,
  COUNT(*) AS total_records,
  COUNT(DISTINCT CONCAT(product_id, '|', store_id)) AS unique_entities,
  COUNT(*) - COUNT(DISTINCT CONCAT(product_id, '|', store_id)) AS duplicate_entity_count,
  COUNTIF(product_id IS NULL OR store_id IS NULL) AS null_entity_ids,
  COUNTIF(quantity < 0) AS negative_amounts,
  COUNTIF(dq_status = 'INVALID') AS dq_invalid_rows,
  COUNTIF(dq_status IS NULL)     AS dq_status_null_rows,
  AVG(TIMESTAMP_DIFF(ingestion_ts, last_updated, HOUR)) AS avg_arrival_delay_hours
FROM `giga_raw.raw_inventory_snapshots`
WHERE ingestion_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);

-- Late arriving data summary (last 30 days)
CREATE OR REPLACE VIEW `giga_curated.v_late_arriving_data_summary` AS
SELECT
  table_name,
  DATE(ingestion_timestamp) AS ingestion_date,
  COUNT(*) AS total_late_records,
  AVG(arrival_delay_seconds) AS avg_delay_seconds,
  MAX(arrival_delay_seconds) AS max_delay_seconds,
  SUM(CASE WHEN processed THEN 1 ELSE 0 END) AS processed_count,
  SUM(CASE WHEN NOT processed THEN 1 ELSE 0 END) AS pending_count
FROM `giga_curated.late_arriving_data`
WHERE ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY table_name, DATE(ingestion_timestamp)
ORDER BY ingestion_date DESC, table_name;
