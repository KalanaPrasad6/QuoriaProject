set -euo pipefail

PROJECT_ID="${PROJECT_ID:-quoriagigamartproject}"
REGION="${REGION:-asia-south1}"

BQ_LOCATION="${BQ_LOCATION:-asia-south1}"

RAW_BUCKET="${RAW_BUCKET:-giga-raw-quoriagigamartproject}"
STAGING_BUCKET="${STAGING_BUCKET:-giga-dataflow-staging-quoriagigamartproject}"
TEMP_BUCKET="${TEMP_BUCKET:-giga-dataflow-temp-quoriagigamartproject}"

DATAFLOW_SA_NAME="${DATAFLOW_SA_NAME:-gigamart-dataflow-sa}"
COMPOSER_SA_NAME="${COMPOSER_SA_NAME:-gigamart-composer-sa}"

gcloud config set project "$PROJECT_ID"

PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"

DATAFLOW_SA="${DATAFLOW_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
COMPOSER_SA="${COMPOSER_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

COMPOSER_SERVICE_AGENT="service-${PROJECT_NUMBER}@cloudcomposer-accounts.iam.gserviceaccount.com"
GOOGLE_APIS_SA="${PROJECT_NUMBER}@cloudservices.gserviceaccount.com"
DATAFLOW_SERVICE_AGENT="service-${PROJECT_NUMBER}@dataflow-service-producer-prod.iam.gserviceaccount.com"

gsutil mb -l "$REGION" "gs://${RAW_BUCKET}"     || echo "Bucket ${RAW_BUCKET} already exists"
gsutil mb -l "$REGION" "gs://${STAGING_BUCKET}" || echo "Bucket ${STAGING_BUCKET} already exists"
gsutil mb -l "$REGION" "gs://${TEMP_BUCKET}"    || echo "Bucket ${TEMP_BUCKET} already exists"

gcloud pubsub topics create pos-transactions --quiet        || echo "Topic pos-transactions already exists"
gcloud pubsub subscriptions create pos-transactions-sub \
  --topic=pos-transactions --quiet                          || echo "Subscription pos-transactions-sub already exists"

bq --project_id="$PROJECT_ID" --location="$BQ_LOCATION" query --use_legacy_sql=false < sql/00_create_datasets.sql
bq --project_id="$PROJECT_ID" --location="$BQ_LOCATION" query --use_legacy_sql=false < sql/01_create_raw_tables.sql
bq --project_id="$PROJECT_ID" --location="$BQ_LOCATION" query --use_legacy_sql=false < sql/02_create_curated_tables.sql
bq --project_id="$PROJECT_ID" --location="$BQ_LOCATION" query --use_legacy_sql=false < sql/03_sp_customer_dim_scd2.sql
bq --project_id="$PROJECT_ID" --location="$BQ_LOCATION" query --use_legacy_sql=false < sql/05_deduplication_procedure.sql
bq --project_id="$PROJECT_ID" --location="$BQ_LOCATION" query --use_legacy_sql=false < sql/07_late_arriving_data.sql
bq --project_id="$PROJECT_ID" --location="$BQ_LOCATION" query --use_legacy_sql=false < sql/08_monitoring_queries.sql
bq --project_id="$PROJECT_ID" --location="$BQ_LOCATION" query --use_legacy_sql=false < sql/04_optimize_tables.sql

gcloud iam service-accounts create "${DATAFLOW_SA_NAME}" \
  --project="${PROJECT_ID}" \
  --display-name="GigaMart Dataflow Worker SA" \
  || echo "Service account ${DATAFLOW_SA} already exists"

gcloud iam service-accounts create "${COMPOSER_SA_NAME}" \
  --project="${PROJECT_ID}" \
  --display-name="GigaMart Composer Environment SA" \
  || echo "Service account ${COMPOSER_SA} already exists"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${DATAFLOW_SA}" \
  --role="roles/dataflow.worker" \
  --quiet || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${DATAFLOW_SA}" \
  --role="roles/storage.objectAdmin" \
  --quiet || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${DATAFLOW_SA}" \
  --role="roles/bigquery.dataEditor" \
  --quiet || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${DATAFLOW_SA}" \
  --role="roles/pubsub.subscriber" \
  --quiet || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/composer.worker" \
  --quiet || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/bigquery.admin" \
  --quiet || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/dataflow.admin" \
  --quiet || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/storage.objectAdmin" \
  --quiet || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/pubsub.admin" \
  --quiet || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/iam.serviceAccountUser" \
  --quiet || true

gcloud iam service-accounts add-iam-policy-binding "${DATAFLOW_SA}" \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/iam.serviceAccountUser" \
  --quiet || true

COMPOSER_SERVICE_AGENT="service-${PROJECT_NUMBER}@cloudcomposer-accounts.iam.gserviceaccount.com"
GOOGLE_APIS_SA="${PROJECT_NUMBER}@cloudservices.gserviceaccount.com"
DATAFLOW_SERVICE_AGENT="service-${PROJECT_NUMBER}@dataflow-service-producer-prod.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${COMPOSER_SERVICE_AGENT}" \
  --role="roles/composer.serviceAgent" \
  --quiet || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${DATAFLOW_SERVICE_AGENT}" \
  --role="roles/dataflow.serviceAgent" \
  --quiet || true

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${GOOGLE_APIS_SA}" \
  --role="roles/serviceusage.serviceUsageAdmin" \
  --quiet || true

echo "    Dataflow SA:  ${DATAFLOW_SA}"
echo "    Composer SA:  ${COMPOSER_SA}"
echo "    Buckets:      ${RAW_BUCKET}, ${STAGING_BUCKET}, ${TEMP_BUCKET}"
