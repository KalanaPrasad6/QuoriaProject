CREATE TABLE IF NOT EXISTS `giga_curated.customer_dim_scd2` (
  customer_sk      STRING NOT NULL,
  customer_id      STRING NOT NULL,
  first_name       STRING,
  last_name        STRING,
  email            STRING,
  loyalty_status   STRING,
  membership_tier  STRING,
  city             STRING,
  country          STRING,
  effective_from   TIMESTAMP NOT NULL,
  effective_to     TIMESTAMP,
  is_current       BOOL NOT NULL,
  row_hash         STRING NOT NULL,
  last_updated_ts  TIMESTAMP NOT NULL
)
PARTITION BY DATE(effective_from)
CLUSTER BY customer_id, is_current
OPTIONS (
  description = "Customer dimension with SCD2, optimized for big data queries"
);

CREATE TABLE IF NOT EXISTS `giga_curated.pipeline_monitoring` (
  pipeline_name           STRING NOT NULL,
  run_date                DATE NOT NULL,
  run_timestamp           TIMESTAMP NOT NULL,
  status                  STRING NOT NULL,
  records_processed       INT64,
  records_failed          INT64,
  execution_time_seconds  INT64,
  error_message           STRING,
  metadata                JSON
)
PARTITION BY run_date
CLUSTER BY pipeline_name, status
OPTIONS (
  description = "Pipeline execution monitoring and alerting"
);

CREATE TABLE IF NOT EXISTS `giga_curated.late_arriving_data` (
  table_name           STRING NOT NULL,
  record_key           STRING NOT NULL,
  event_timestamp      TIMESTAMP NOT NULL,
  ingestion_timestamp  TIMESTAMP NOT NULL,
  arrival_delay_seconds INT64,
  processed            BOOL NOT NULL,
  processed_timestamp  TIMESTAMP
)
PARTITION BY DATE(ingestion_timestamp)
CLUSTER BY table_name, processed
OPTIONS (
  description = "Tracks late arriving data for reprocessing"
);
