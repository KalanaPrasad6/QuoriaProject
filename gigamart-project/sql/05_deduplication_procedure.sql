-- Deduplication stored procedures for handling duplicate records

CREATE OR REPLACE PROCEDURE `giga_raw.sp_deduplicate_pos_transactions`()
BEGIN
  -- Rank rows by latest ingestion_ts per (transaction_id, event_ts)
  CREATE TEMP TABLE ranked AS
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id, event_ts
      ORDER BY ingestion_ts DESC
    ) AS rn
  FROM `giga_raw.raw_pos_transactions`;

  -- Log duplicates (rn > 1) into deduplication_log using record_hash where available
  INSERT INTO `giga_raw.deduplication_log` (
    table_name,
    record_hash,
    record_key,
    first_seen,
    last_seen,
    count
  )
  SELECT
    'raw_pos_transactions' AS table_name,
    record_hash,
    CONCAT(transaction_id, '|', CAST(event_ts AS STRING)) AS record_key,
    MIN(ingestion_ts) AS first_seen,
    MAX(ingestion_ts) AS last_seen,
    COUNT(*) AS count
  FROM ranked
  WHERE rn > 1
    AND record_hash IS NOT NULL
  GROUP BY record_hash, transaction_id, event_ts;

  -- Remove duplicates, keeping rn = 1
  DELETE FROM `giga_raw.raw_pos_transactions`
  WHERE (transaction_id, event_ts, ingestion_ts) IN (
    SELECT transaction_id, event_ts, ingestion_ts
    FROM ranked
    WHERE rn > 1
  );
END;

-- ECOM: deduplicate on order_id keeping latest ingestion_ts
CREATE OR REPLACE PROCEDURE `giga_raw.sp_deduplicate_ecom_orders`()
BEGIN
  CREATE TEMP TABLE ranked AS
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY ingestion_ts DESC
    ) AS rn
  FROM `giga_raw.raw_ecom_orders`;

  -- Log duplicates to deduplication_log
  INSERT INTO `giga_raw.deduplication_log` (
    table_name,
    record_hash,
    record_key,
    first_seen,
    last_seen,
    count
  )
  SELECT
    'raw_ecom_orders' AS table_name,
    record_hash,
    order_id AS record_key,
    MIN(ingestion_ts) AS first_seen,
    MAX(ingestion_ts) AS last_seen,
    COUNT(*) AS count
  FROM ranked
  WHERE rn > 1
    AND record_hash IS NOT NULL
  GROUP BY record_hash, order_id;

  DELETE FROM `giga_raw.raw_ecom_orders`
  WHERE (order_id, ingestion_ts) IN (
    SELECT order_id, ingestion_ts
    FROM ranked
    WHERE rn > 1
  );
END;

-- CRM: deduplicate on (customer_id, updated_at) keeping latest ingestion_ts
CREATE OR REPLACE PROCEDURE `giga_raw.sp_deduplicate_crm_customers`()
BEGIN
  CREATE TEMP TABLE ranked AS
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id, updated_at
      ORDER BY ingestion_ts DESC
    ) AS rn
  FROM `giga_raw.raw_crm_customers`;

  DELETE FROM `giga_raw.raw_crm_customers`
  WHERE (customer_id, updated_at, ingestion_ts) IN (
    SELECT customer_id, updated_at, ingestion_ts
    FROM ranked
    WHERE rn > 1
  );
END;
