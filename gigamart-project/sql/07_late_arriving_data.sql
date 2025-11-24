-- Procedure to handle late arriving data
CREATE OR REPLACE PROCEDURE `giga_curated.sp_process_late_arriving_data`(
  table_name_param STRING,
  lookback_days INT64
)
BEGIN
  DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE cutoff_ts TIMESTAMP DEFAULT TIMESTAMP_SUB(run_ts, INTERVAL lookback_days DAY);

  -- Identify late arriving records from RAW tables (arrived > 1 hour after event time)
  CREATE TEMP TABLE late_records AS
  SELECT
    src.table_name,
    src.key AS record_key,
    src.event_ts AS event_timestamp,
    src.ingestion_ts AS ingestion_timestamp,
    TIMESTAMP_DIFF(src.ingestion_ts, src.event_ts, SECOND) AS arrival_delay_seconds
  FROM (
    -- POS
    SELECT
      'raw_pos_transactions' AS table_name,
      transaction_id AS key,
      event_ts,
      ingestion_ts
    FROM `giga_raw.raw_pos_transactions`
    WHERE ingestion_ts >= cutoff_ts
      AND event_ts IS NOT NULL
      AND TIMESTAMP_DIFF(ingestion_ts, event_ts, HOUR) > 1

    UNION ALL

    -- ECOM
    SELECT
      'raw_ecom_orders' AS table_name,
      order_id AS key,
      order_ts AS event_ts,
      ingestion_ts
    FROM `giga_raw.raw_ecom_orders`
    WHERE ingestion_ts >= cutoff_ts
      AND order_ts IS NOT NULL
      AND TIMESTAMP_DIFF(ingestion_ts, order_ts, HOUR) > 1

    UNION ALL

    -- CRM
    SELECT
      'raw_crm_customers' AS table_name,
      customer_id AS key,
      updated_at AS event_ts,
      ingestion_ts
    FROM `giga_raw.raw_crm_customers`
    WHERE ingestion_ts >= cutoff_ts
      AND updated_at IS NOT NULL
      AND TIMESTAMP_DIFF(ingestion_ts, updated_at, HOUR) > 1
  ) AS src
  WHERE
    -- If table_name_param is NULL or 'all', process all tables.
    -- Otherwise, filter to the given table only.
    (table_name_param IS NULL OR table_name_param = 'all' OR src.table_name = table_name_param);

  -- Upsert late records into tracking table using MERGE
  MERGE `giga_curated.late_arriving_data` AS T
  USING (
    SELECT
      table_name,
      record_key,
      event_timestamp,
      ingestion_timestamp,
      arrival_delay_seconds
    FROM late_records
    WHERE arrival_delay_seconds > 3600  -- more than 1 hour delay
  ) AS S
  ON T.table_name          = S.table_name
     AND T.record_key      = S.record_key
     AND T.event_timestamp = S.event_timestamp
     AND T.ingestion_timestamp = S.ingestion_timestamp
  WHEN MATCHED THEN
    UPDATE SET
      T.arrival_delay_seconds = GREATEST(T.arrival_delay_seconds, S.arrival_delay_seconds)
  WHEN NOT MATCHED THEN
    INSERT (
      table_name,
      record_key,
      event_timestamp,
      ingestion_timestamp,
      arrival_delay_seconds,
      processed,
      processed_timestamp
    )
    VALUES (
      S.table_name,
      S.record_key,
      S.event_timestamp,
      S.ingestion_timestamp,
      S.arrival_delay_seconds,
      FALSE,
      NULL
    );

  -- Log late arriving data metrics
  INSERT INTO `giga_curated.pipeline_monitoring` (
    pipeline_name,
    run_date,
    run_timestamp,
    status,
    records_processed,
    records_failed,
    execution_time_seconds,
    metadata
  )
  SELECT
    'sp_process_late_arriving_data' AS pipeline_name,
    DATE(run_ts)                    AS run_date,
    run_ts                          AS run_timestamp,
    'SUCCESS'                       AS status,
    COUNT(*)                        AS records_processed,
    0                               AS records_failed,
    CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), run_ts, SECOND) AS INT64) AS execution_time_seconds,
    JSON_OBJECT(
      'table_name_param', table_name_param,
      'avg_delay_seconds', CAST(AVG(arrival_delay_seconds) AS INT64),
      'max_delay_seconds', CAST(MAX(arrival_delay_seconds) AS INT64)
    ) AS metadata
  FROM late_records;
END;
