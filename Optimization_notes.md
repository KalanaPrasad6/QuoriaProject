# GigaMart Data Platform – Optimization Notes

Cost and performance tuning for BigQuery, Dataflow, Composer, and storage.

---

## 1. BigQuery

### 1.1 Partitioning & clustering

- Ensure all large tables are **partitioned**:
  - `giga_raw.raw_pos_transactions` by `event_date`
  - `giga_raw.raw_ecom_orders` by `order_date`
  - `giga_raw.raw_crm_customers` by `DATE(updated_at)`
  - `giga_raw.raw_inventory_snapshots` by snapshot date
- Use **clustering** on common filters/joins:
  - POS/E-com facts: `store_id`, `customer_id`, `product_id`
  - CRM: `customer_id`, `email`

### 1.2 Query patterns

- Avoid `SELECT *`; project only needed columns.
- Always filter by partitions: `WHERE event_date BETWEEN ...`.
- Use **materialized views** or scheduled queries for heavy, repeated aggregates.

### 1.3 Table design

- Keep fact tables narrow and use SCD2 dimensions for attributes.
- Use surrogate keys instead of wide natural keys.

### 1.4 Cost controls

- Monitor query costs via `INFORMATION_SCHEMA.JOBS_BY_*` views.
- Educate users to:
  - Limit time ranges.
  - Avoid cross-join patterns.
- Consider Reservations / Flex Slots when workload stabilizes.

---

## 2. Dataflow

### 2.1 General

- Use consistent region: `--region=asia-south1`.
- Start with modest workers (`n1-standard-2` or `n2-standard-2`) and enable autoscaling.
- Use **Runner v2** and Streaming Engine where appropriate.

### 2.2 Streaming (POS)

- Use tight windows (e.g. 1–5 minutes) with reasonable `allowed_lateness` (e.g. 60 minutes).
- Avoid global aggregations; use keyed/windowed aggregations instead.
- Route malformed messages to a dead-letter table instead of failing the pipeline.

### 2.3 Batch

- Read only **new files** with date-based prefixes or a “processed files” table.
- Avoid thousands of tiny files; compact upstream where possible.
- Cap `max_num_workers` and tune based on runtime and cost.

### 2.4 Error handling

- Use dead-letter tables for each pipeline (via parameters in metadata JSON).
- Periodically analyze DLQ tables and fix upstream data issues.

---

## 3. Cloud Composer

### 3.1 Environment sizing

- Start with small Composer environment (few workers, minimal scheduler resources).
- Disable DAGs not needed in each environment (dev vs prod).

### 3.2 DAG efficiency

- Use deferrable or sensor-friendly operators to avoid long-running busy-wait tasks.
- Offload heavy logic to BigQuery/Dataflow; keep DAGs as orchestration only.
- Parameterize environment using variables/connections instead of hardcoding.

### 3.3 Alerting

- Use Airflow `EmailOperator` for failures.
- Optionally add Slack/Teams alerts using Webhook operators.

---

## 4. Storage & Lifecycle

### 4.1 GCS buckets

- Add lifecycle policies:
  - Raw bucket: move old objects to Nearline/Coldline if reprocessing is rare.
  - Staging/temp buckets: delete objects older than 7–30 days.
- Keep file sizes reasonable (tens of MB) to balance overhead vs parallelism.

### 4.2 BigQuery retention

- For very old raw data, consider:
  - Exporting to GCS (Parquet) and dropping rarely queried partitions.
  - Or moving them to a cheaper archival dataset.

---

## 5. Data Quality & Late Data

- Use `record_hash`-based deduplication in both Beam and BigQuery procedures.
- Track late arrivals in `giga_curated.late_arriving_data` and decide:
  - Whether to backfill facts.
  - Or adjust KPIs only for specific critical use cases.

---

## 6. Governance & Cost Management

- Apply labels to jobs/datasets (e.g. `env=dev/prod`, `system=gigamart`, `owner=data-eng`) to slice costs.
- Use budget alerts on the project and on specific services (BigQuery, Dataflow, Composer).
- Limit BigQuery write permissions to pipeline service accounts; keep analysts read-only on curated/mart layers.

---

## 7. Future Optimizations

- Enable BigQuery BI Engine on key mart tables to speed up dashboards.
- Add data contracts and schema validation at ingestion.
- Add end-to-end integration tests to validate KPIs after pipeline changes.
