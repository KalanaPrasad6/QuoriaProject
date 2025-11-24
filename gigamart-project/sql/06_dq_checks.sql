-- Comprehensive Data Quality Checks

DECLARE pos_null_txn       INT64;
DECLARE ecom_null_order    INT64;
DECLARE crm_null_customer  INT64;
DECLARE inv_null_product   INT64;
DECLARE inv_null_store     INT64;

DECLARE pos_negative_amount  INT64;
DECLARE ecom_negative_amount INT64;

DECLARE pos_future_date    INT64;
DECLARE ecom_future_date   INT64;

DECLARE pos_duplicates     INT64;
DECLARE ecom_duplicates    INT64;

DECLARE crm_invalid_email  INT64;
DECLARE inv_negative_qty   INT64;

-- counts of DQ-flagged rows
DECLARE pos_invalid_dq   INT64;
DECLARE ecom_invalid_dq  INT64;
DECLARE crm_invalid_dq   INT64;
DECLARE inv_invalid_dq   INT64;

-- Check for null required fields
SET pos_null_txn = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_pos_transactions`
  WHERE transaction_id IS NULL
);

SET ecom_null_order = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_ecom_orders`
  WHERE order_id IS NULL
);

SET crm_null_customer = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_crm_customers`
  WHERE customer_id IS NULL
);

SET inv_null_product = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_inventory_snapshots`
  WHERE product_id IS NULL
);

SET inv_null_store = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_inventory_snapshots`
  WHERE store_id IS NULL
);

-- Check for negative amounts
SET pos_negative_amount = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_pos_transactions`
  WHERE amount < 0
);

SET ecom_negative_amount = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_ecom_orders`
  WHERE total_amount < 0
);

-- Check for future dates
SET pos_future_date = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_pos_transactions`
  WHERE event_ts > CURRENT_TIMESTAMP()
);

SET ecom_future_date = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_ecom_orders`
  WHERE order_ts > CURRENT_TIMESTAMP()
);

-- Check for duplicate IDs
SET pos_duplicates = (
  SELECT COUNT(*) - COUNT(DISTINCT transaction_id)
  FROM `giga_raw.raw_pos_transactions`
  WHERE transaction_id IS NOT NULL
);

SET ecom_duplicates = (
  SELECT COUNT(*) - COUNT(DISTINCT order_id)
  FROM `giga_raw.raw_ecom_orders`
  WHERE order_id IS NOT NULL
);

-- Check for invalid email formats (basic check)
SET crm_invalid_email = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_crm_customers`
  WHERE email IS NOT NULL
    AND email NOT LIKE '%@%.%'
);

-- Check for negative inventory quantities
SET inv_negative_qty = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_inventory_snapshots`
  WHERE quantity < 0
);

-- DQ status-based counts
SET pos_invalid_dq = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_pos_transactions`
  WHERE dq_status = 'INVALID'
);

SET ecom_invalid_dq = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_ecom_orders`
  WHERE dq_status = 'INVALID'
);

SET crm_invalid_dq = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_crm_customers`
  WHERE dq_status = 'INVALID'
);

SET inv_invalid_dq = (
  SELECT COUNT(*)
  FROM `giga_raw.raw_inventory_snapshots`
  WHERE dq_status = 'INVALID'
);

-- Output results as a single row
SELECT
  pos_null_txn       AS pos_null_transaction_id_count,
  ecom_null_order    AS ecom_null_order_id_count,
  crm_null_customer  AS crm_null_customer_id_count,
  inv_null_product   AS inv_null_product_id_count,
  inv_null_store     AS inv_null_store_id_count,
  pos_negative_amount  AS pos_negative_amount_count,
  ecom_negative_amount AS ecom_negative_amount_count,
  pos_future_date    AS pos_future_date_count,
  ecom_future_date   AS ecom_future_date_count,
  pos_duplicates     AS pos_duplicate_transaction_count,
  ecom_duplicates    AS ecom_duplicate_order_count,
  crm_invalid_email  AS crm_invalid_email_count,
  inv_negative_qty   AS inv_negative_quantity_count,
  pos_invalid_dq     AS pos_invalid_dq_row_count,
  ecom_invalid_dq    AS ecom_invalid_dq_row_count,
  crm_invalid_dq     AS crm_invalid_dq_row_count,
  inv_invalid_dq     AS inv_invalid_dq_row_count;
