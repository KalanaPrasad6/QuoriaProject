CREATE OR REPLACE PROCEDURE `giga_curated.sp_upsert_customer_dim_scd2`()
BEGIN
  DECLARE run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE records_processed INT64 DEFAULT 0;
  DECLARE records_inserted  INT64 DEFAULT 0;
  DECLARE records_updated   INT64 DEFAULT 0;

  -- 1. Source: deduplicate recent CRM raw records and ignore invalid DQ rows
  CREATE TEMP TABLE deduplicated_source AS
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY customer_id, updated_at
        ORDER BY ingestion_ts DESC
      ) AS rn
    FROM `giga_raw.raw_crm_customers`
    WHERE updated_at IS NOT NULL
      AND updated_at >= TIMESTAMP_SUB(run_ts, INTERVAL 90 DAY)
      AND (dq_status IS NULL OR dq_status = 'VALID')
  )
  WHERE rn = 1;

  -- 2. Collapse to one current row per customer_id with a stable row_hash
  CREATE TEMP TABLE new_customers AS
  SELECT
    customer_id,
    ARRAY_AGG(first_name       ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS first_name,
    ARRAY_AGG(last_name        ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS last_name,
    ARRAY_AGG(email            ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS email,
    ARRAY_AGG(loyalty_status   ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS loyalty_status,
    ARRAY_AGG(membership_tier  ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS membership_tier,
    ARRAY_AGG(city             ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS city,
    ARRAY_AGG(country          ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS country,
    MAX(updated_at) AS updated_at,
    TO_HEX(MD5(TO_JSON_STRING(STRUCT(
      ARRAY_AGG(first_name       ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS first_name,
      ARRAY_AGG(last_name        ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS last_name,
      ARRAY_AGG(email            ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS email,
      ARRAY_AGG(loyalty_status   ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS loyalty_status,
      ARRAY_AGG(membership_tier  ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS membership_tier,
      ARRAY_AGG(city             ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS city,
      ARRAY_AGG(country          ORDER BY updated_at DESC LIMIT 1)[OFFSET(0)] AS country
    )))) AS row_hash
  FROM deduplicated_source
  GROUP BY customer_id;

  SET records_processed = (SELECT COUNT(*) FROM new_customers);

  -- 3. Snapshot current SCD2 state
  CREATE TEMP TABLE current_dim AS
  SELECT *
  FROM `giga_curated.customer_dim_scd2`
  WHERE is_current = TRUE;

  -- 4. Customers whose current row must be expired (hash changed)
  CREATE TEMP TABLE customers_to_expire AS
  SELECT d.customer_sk, d.customer_id
  FROM current_dim d
  JOIN new_customers n USING (customer_id)
  WHERE d.row_hash <> n.row_hash;

  -- 5. New SCD2 rows to insert (new customer or changed attributes)
  CREATE TEMP TABLE customers_to_insert AS
  SELECT n.*
  FROM new_customers n
  LEFT JOIN current_dim d USING (customer_id)
  WHERE d.customer_id IS NULL
     OR d.row_hash <> n.row_hash;

  -- 6. Expire old rows
  UPDATE `giga_curated.customer_dim_scd2` d
  SET d.effective_to = run_ts,
      d.is_current   = FALSE
  WHERE EXISTS (
    SELECT 1
    FROM customers_to_expire e
    WHERE e.customer_sk = d.customer_sk
  );

  SET records_updated = (SELECT COUNT(*) FROM customers_to_expire);

  -- 7. Insert new current rows
  INSERT INTO `giga_curated.customer_dim_scd2` (
    customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    loyalty_status,
    membership_tier,
    city,
    country,
    effective_from,
    effective_to,
    is_current,
    row_hash,
    last_updated_ts
  )
  SELECT
    GENERATE_UUID() AS customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    loyalty_status,
    membership_tier,
    city,
    country,
    run_ts AS effective_from,
    NULL   AS effective_to,
    TRUE   AS is_current,
    row_hash,
    updated_at AS last_updated_ts
  FROM customers_to_insert;

  SET records_inserted = (SELECT COUNT(*) FROM customers_to_insert);

  -- 8. Log execution into pipeline_monitoring
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
  VALUES (
    'sp_upsert_customer_dim_scd2',
    DATE(run_ts),
    run_ts,
    'SUCCESS',
    records_processed,
    0,
    CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), run_ts, SECOND) AS INT64),
    JSON_OBJECT(
      'records_inserted', records_inserted,
      'records_updated',  records_updated
    )
  );
END;
