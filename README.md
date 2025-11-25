# GigaMart Data Platform on GCP

End-to-end retail analytics platform using:

- **Apache Beam / Dataflow** (batch + streaming)
- **Pub/Sub** (POS events)
- **Cloud Storage** (landing zone)
- **BigQuery** (warehouse – Bronze / Silver / Gold)
- **Cloud Composer (Airflow)** (orchestration)

---

## 1. Repository Structure

```text
gigamart-project/
  config/          # dev/prod YAML configs
  dags/            # Airflow DAGs for Composer
  data/samples/    # Sample CSV/JSON data
  infra/           # Shell scripts for infra & deployment
  metadata/        # Dataflow Flex template metadata JSONs
  sql/             # BigQuery DDL, SPs, DQ & monitoring queries
  src/             # Apache Beam pipelines (batch & streaming)
  requirements.txt # Python dependencies
```

---

## 2. Prerequisites

- GCP project.
- `gcloud` CLI configured.
- Python 3.11+ for local dev.
- APIs enabled: BigQuery, Dataflow, Pub/Sub, Composer, Artifact Registry, Cloud Storage.

---

## 3. Quick Start (Dev)

### 3.1 Set environment variables

```bash
cd gigamart-project

export PROJECT_ID=quoriagigamartproject
export REGION=asia-south1
export BQ_LOCATION=asia-south1

export RAW_BUCKET=giga-raw-quoriagigamartproject
export STAGING_BUCKET=giga-dataflow-staging-quoriagigamartproject
export TEMP_BUCKET=giga-dataflow-temp-quoriagigamartproject

export DATAFLOW_SA_NAME=gigamart-dataflow-sa
export COMPOSER_SA_NAME=gigamart-composer-sa
export ENV_NAME=gigamart-composer
```

### 3.2 Enable APIs

```bash
./infra/01_enable_apis.sh
```

### 3.3 Create core resources

```bash
./infra/02_create_core_resources.sh
```

This script creates:

- Buckets (`RAW_BUCKET`, `STAGING_BUCKET`, `TEMP_BUCKET`)
- BigQuery datasets: `giga_raw`, `giga_curated`, `giga_mart`
- Core tables & procedures via `sql/*.sql`
- Service accounts & IAM roles
- Pub/Sub topic/subscription for POS

### 3.4 Build Dataflow images & Flex templates

```bash
./infra/03_build_images_and_templates.sh
```

Templates are written to:

- `gs://${TEMP_BUCKET}/flex-templates/*.json`

### 3.5 Create Composer environment

```bash
./infra/04_create_composer_env.sh
```

### 3.6 Deploy DAGs & SQL to Composer

```bash
./infra/05_deploy_dags_and_sql.sh
```

---

## 4. Load Sample Data

```bash
# CRM
gsutil cp data/samples/crm_customers_2025_10_20.csv gs://${RAW_BUCKET}/crm-profiles/

# E-commerce
gsutil cp data/samples/ecommerce_orders_2025_10_20.json gs://${RAW_BUCKET}/ecom-orders/

# Inventory
gsutil cp data/samples/inventory_snapshot_2025_10_20.csv gs://${RAW_BUCKET}/inventory/

# POS (batch)
gsutil cp data/samples/pos_data_day1.json gs://${RAW_BUCKET}/pos/
```

Publish a sample POS event to Pub/Sub:

```bash
gcloud pubsub topics publish pos-transactions --message '{
  "transaction_id": "TXN001",
  "store_id": "S001",
  "customer_id": "C1001",
  "amount": 45.75,
  "currency": "USD",
  "timestamp": "2025-10-20T09:12:34Z"
}'
```

---

## 5. Running Pipelines

### Using Composer

In the Airflow UI for `gigamart-composer`:

1. Enable `gigamart_daily_batch_dag` and trigger a run.
2. Enable `gigamart_hourly_ecommerce_dag`.
3. Enable `gigamart_realtime_pos_dag` to start streaming POS.
4. Enable `gigamart_monitoring_dag`.

### Direct Flex template example (CRM)

```bash
JOB_NAME="crm-csv-to-bq-$(date +%Y%m%d-%H%M%S)"
TEMPLATE_PATH="gs://${TEMP_BUCKET}/flex-templates/crm_batch.json"

gcloud dataflow flex-template run "$JOB_NAME"   --project="$PROJECT_ID"   --region="$REGION"   --template-file-gcs-location="$TEMPLATE_PATH"   --staging-location="gs://${STAGING_BUCKET}/staging"   --parameters       input_pattern="gs://${RAW_BUCKET}/crm-profiles/*.csv",      output_table="${PROJECT_ID}:giga_raw.raw_crm_customers"
```

---

## 6. Local Development

```bash
python3 -m venv venv
source venv/bin/activate

pip install --upgrade pip
pip install -r requirements.txt
```

Run a pipeline locally (DirectRunner) on sample data:

```bash
python src/pipelines/crm_batch.py   --runner DirectRunner   --input_pattern data/samples/crm_customers_2025_10_21.csv   --project "$PROJECT_ID"   --temp_location "/tmp"   --output_table "${PROJECT_ID}:giga_raw.raw_crm_customers"
```

---

## 7. Assumptions

- All core services reside in `asia-south1`.
- DDL in `sql/` is the source of truth for schemas.
- Upstream systems provide data in compatible formats.
