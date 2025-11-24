CREATE TABLE IF NOT EXISTS `giga_raw.raw_pos_transactions` (
  transaction_id STRING NOT NULL,
  store_id       STRING,
  customer_id    STRING,
  amount         FLOAT64,
  currency       STRING,
  event_ts       TIMESTAMP,
  event_date     DATE,
  ingestion_ts   TIMESTAMP NOT NULL,
  source_file    STRING,
  raw_payload    STRING,
  record_hash    STRING, 
  dq_status      STRING,  
  dq_errors      STRING 
)
PARTITION BY event_date
CLUSTER BY store_id, customer_id, transaction_id
OPTIONS (
  description = "Raw POS transactions with DQ flags; partitioned by event_date"
);

CREATE TABLE IF NOT EXISTS `giga_raw.raw_ecom_orders` (
  order_id      STRING NOT NULL,
  customer_id   STRING,
  store_id      STRING,
  total_amount  FLOAT64,
  payment_type  STRING,
  order_ts      TIMESTAMP,
  order_date    DATE,
  ingestion_ts  TIMESTAMP NOT NULL,
  source_file   STRING,
  raw_payload   STRING,
  record_hash   STRING,
  dq_status     STRING,
  dq_errors     STRING
)
PARTITION BY order_date
CLUSTER BY customer_id, store_id, order_id
OPTIONS (
  description = "Raw e-commerce orders with DQ flags; partitioned by order_date"
);

CREATE TABLE IF NOT EXISTS `giga_raw.raw_crm_customers` (
  customer_id         STRING NOT NULL,
  first_name          STRING,
  last_name           STRING,
  email               STRING,
  loyalty_status      STRING,
  city                STRING,
  country             STRING,
  membership_tier     STRING,
  updated_at          TIMESTAMP,
  ingestion_ts        TIMESTAMP NOT NULL,
  source_file         STRING,
  raw_payload         STRING,
  raw_extra_attributes STRING,
  record_hash         STRING,
  dq_status           STRING,
  dq_errors           STRING
)
PARTITION BY DATE(updated_at)
CLUSTER BY customer_id, email
OPTIONS (
  description = "Raw CRM customers with schema-evolution support and DQ flags"
);

CREATE TABLE IF NOT EXISTS `giga_raw.raw_inventory_snapshots` (
  product_id    STRING NOT NULL,
  store_id      STRING NOT NULL,
  product_name  STRING,
  quantity      INT64,
  price         FLOAT64,
  last_updated  TIMESTAMP,
  ingestion_ts  TIMESTAMP NOT NULL,
  source_file   STRING,
  raw_payload   STRING,
  record_hash   STRING,
  dq_status     STRING,
  dq_errors     STRING
)
PARTITION BY DATE(last_updated)
CLUSTER BY product_id, store_id
OPTIONS (
  description = "Raw inventory snapshots with DQ flags; partitioned by last_updated"
);

CREATE TABLE IF NOT EXISTS `giga_raw.deduplication_log` (
  table_name  STRING NOT NULL,
  record_hash STRING NOT NULL,
  record_key  STRING,
  first_seen  TIMESTAMP NOT NULL,
  last_seen   TIMESTAMP NOT NULL,
  count       INT64 NOT NULL
)
PARTITION BY DATE(first_seen)
CLUSTER BY table_name, record_hash
OPTIONS (
  description = "Tracks duplicate records detected by deduplication procedures"
);
