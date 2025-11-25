# GigaMart Retail Analytics – Design Document

## 1. Problem Statement & Goals

GigaMart is a retail business with multiple operational systems (POS, E-commerce, CRM, inventory). Today, data is siloed. The goal is to design and implement a **GCP-native, production-ready data platform** that:

1. Ingests data from POS, E-com, CRM, and Inventory.
2. Stores data in a governed **BigQuery data warehouse** (Bronze/Silver/Gold).
3. Provides **near real-time analytics** for POS and timely insights for other domains.
4. Implements **data quality, deduplication, and SCD2** for dimensions.
5. Is **cost-efficient, observable, and easy to operate** using standard GCP services.

---

## 2. Requirements & Assumptions

### Functional

- Unified data model combining POS, E-com, CRM, and Inventory.
- Medallion architecture:
  - Bronze: raw ingested tables (`giga_raw`).
  - Silver: cleaned & conformed tables (`giga_curated`).
  - Gold: BI-ready marts (`giga_mart`).
- Ingestion frequencies:
  - POS streaming (Pub/Sub + Dataflow).
  - POS batch & inventory snapshots: daily.
  - E-commerce: hourly.
  - CRM: daily or on demand.
- Data quality & governance: deduplication, DQ flags, late-arriving data tracking.
- Orchestration: Cloud Composer (Airflow).

### Non-functional

- Region: **`asia-south1`** for all core services.
- Use managed services (Dataflow, BigQuery, Pub/Sub, Composer).
- Multi-environment via YAML config (`config/config_dev.yaml`, `config/config_prod.yaml`).
- Strong observability (logs, DQ tables, monitoring DAG).

### Assumptions

- Upstream systems can export to GCS or publish to Pub/Sub.
- IAM and networking are configured at project level by platform team.

---

## 3. High-Level Architecture

- **Ingestion**
  - POS events → Pub/Sub (`pos-transactions`) → Dataflow Flex (`pos_streaming`) → `giga_raw.raw_pos_transactions`.
  - E-com orders (JSON) in GCS → Dataflow Flex (`ecommerce_batch`) → `giga_raw.raw_ecom_orders`.
  - CRM customers (CSV) in GCS → Dataflow Flex (`crm_batch`) → `giga_raw.raw_crm_customers`.
  - Inventory snapshots (CSV) in GCS → Dataflow Flex (`inventory_batch`) → `giga_raw.raw_inventory_snapshots`.

- **Storage & Modeling**
  - Bronze: ingestion-time partitioned raw tables with `raw_payload`, `record_hash`, DQ flags.
  - Silver: curated SCD2 dimensions and fact tables in `giga_curated`.
  - Gold: BI marts and views in `giga_mart`

- **Orchestration**
  - Composer environment `gigamart-composer` runs DAGs:
    - `gigamart_daily_batch_dag`
    - `gigamart_hourly_ecommerce_dag`
    - `gigamart_realtime_pos_dag`
    - `gigamart_monitoring_dag`

- **Observability**
  - Cloud Logging & Error Reporting.
  - BigQuery monitoring tables (dedup log, late data, DQ checks).

---

## 4. Ingestion Pipelines (Dataflow)

All pipelines live under `src/pipelines/` and are deployed as Flex templates via `infra/03_build_images_and_templates.sh` and `metadata/*.json`.

### POS Streaming – `pos_streaming`

- Source: Pub/Sub subscription `pos-transactions-sub`.
- Sink: `giga_raw.raw_pos_transactions`.
- Features:
  - Fixed windows and watermarks.
  - Enrichment with `event_date`, `ingestion_ts`, `record_hash`.
  - Optional dead-letter table for malformed events.

### POS Batch – `pos_batch`

- Source: JSON files in `gs://giga-raw-*/pos/`.
- Sink: same table as streaming (`raw_pos_transactions`).
- Use: backfill and daily catch-up for stores not streaming.

### E-commerce Batch – `ecommerce_batch`

- Source: JSON files in `gs://giga-raw-*/ecom-orders/`.
- Sink: `giga_raw.raw_ecom_orders`.
- Normalizes orders, enriches with metadata and DQ flags.
- Triggered hourly by the hourly DAG.

### CRM Batch – `crm_batch`

- Source: CSV exports in `gs://giga-raw-*/crm-profiles/`.
- Sink: `giga_raw.raw_crm_customers`.
- Handles schema evolution via `raw_extra_attributes` JSON string.
- Normalizes name/email, adds `updated_at`, `ingestion_ts`, `record_hash`.

### Inventory Batch – `inventory_batch`

- Source: CSV snapshots in `gs://giga-raw-*/inventory/`.
- Sink: `giga_raw.raw_inventory_snapshots`.
- Each run captures stock-on-hand per product and store.

---

## 5. BigQuery Modeling

### Bronze – `giga_raw`

- Partitioned tables:
  - `raw_pos_transactions` (by `event_date`).
  - `raw_ecom_orders` (by `order_date`).
  - `raw_crm_customers` (by `DATE(updated_at)`).
  - `raw_inventory_snapshots` (by snapshot date).
- Common columns:
  - `raw_payload`, `record_hash`, `dq_status`, `dq_errors`, `source_file`, `ingestion_ts`.
- Deduplication and DQ logic driven from stored procedures in `sql/` and common Beam utilities.

### Silver – `giga_curated`

- Key tables:
  - `customer_dim_scd2` – Slowly Changing Dimension Type 2 for customers.
  - `store_dim`, `product_dim`, `date_dim`.
  - `sales_fact` (POS + E-com).
  - `inventory_snapshot_fact`.
- `customer_dim_scd2`:
  - Columns: `customer_sk`, `customer_id`, attributes (name, email, loyalty, city, country), `effective_from`, `effective_to`, `is_current`, `row_hash`, `last_updated_ts`.
  - Maintain via stored procedure `sp_upsert_customer_dim_scd2`.

### Gold – `giga_mart`

- BI-facing tables/views:
  - `daily_store_sales` – store x day aggregates.
  - `customer_360` – joins customer SCD2 with facts.
  - `inventory_kpi` – stock levels, out-of-stock metrics.
- Optimized for dashboard filter patterns and frequently used KPIs.

---

## 6. Orchestration – Cloud Composer (Airflow)

### DAGs

1. **`gigamart_daily_batch_dag`**
   - Runs CRM, inventory, and POS batch Dataflow templates.
   - Executes BigQuery stored procedures for dedup, SCD2, DQ, and late data.
   - On failure, sends email to `EMAIL_ON_FAILURE` from environment variables.

2. **`gigamart_hourly_ecommerce_dag`**
   - Triggers `ecommerce_batch` every hour.
   - Runs dedup/DQ checks for the latest partition.

3. **`gigamart_realtime_pos_dag`**
   - Starts/monitors `pos_streaming` Dataflow job.
   - Periodically executes dedup logic for streaming POS.

4. **`gigamart_monitoring_dag`**
   - Runs DQ and freshness queries from `sql/06_dq_checks.sql` and `sql/08_monitoring_queries.sql`.
   - Raises alerts if freshness, volume, or error thresholds are breached.

---

## 7. Tool Choices & Trade-offs

- **Dataflow**  
  - Dataflow (Apache Beam) for unified batch + streaming and managed scaling.  
  - Trade-off: more complex than “SQL-only” ETL but highly flexible.

- **Pub/Sub for POS**  
  - Decouples producers/consumers and allows multiple downstream use cases.  
  - Adds another managed component to operate.

- **Cloud**  
  - Composer for complex DAG dependencies, backfills, and observability.  
  - Trade-off: higher baseline cost than simple cron/scheduler.

- **Medallion**  
  - Bronze/Curated/Mart for clear lineage, auditability, and evolution.  
  - Trade-off: more tables and transformations, slightly higher storage cost.

---

## 8. Risks & Mitigations

- **Cost spikes (Dataflow/BigQuery)**  
  - Mitigation: small worker types, autoscaling, strict partition filters, scheduled/limited heavy queries.

- **Schema drift from upstream systems**  
  - Mitigation: `raw_extra_attributes` in CRM, versioned contracts, DQ alerts.

- **Data quality issues (duplicates, late data)**  
  - Mitigation: central dedup logic, `late_arriving_data` table, monitoring DAG alerts.

- **Composer environment mis-sizing**  
  - Mitigation: start small, scale based on metrics, disable unused DAGs.
