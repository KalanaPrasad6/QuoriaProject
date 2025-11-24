set -euo pipefail

PROJECT_ID="${PROJECT_ID:-quoriagigamartproject}"
REGION="${REGION:-asia-south1}"
ENV_NAME="${ENV_NAME:-gigamart-composer}"

# Service accounts created in 02_create_core_resources.sh

COMPOSER_SA_NAME="${COMPOSER_SA_NAME:-gigamart-composer-sa}"
DATAFLOW_SA_NAME="${DATAFLOW_SA_NAME:-gigamart-dataflow-sa}"

COMPOSER_SA="${COMPOSER_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
DATAFLOW_SA="${DATAFLOW_SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Buckets created in 02_create_core_resources.sh

RAW_BUCKET="${RAW_BUCKET:-giga-raw-quoriagigamartproject}"
STAGING_BUCKET="${STAGING_BUCKET:-giga-dataflow-staging-quoriagigamartproject}"
TEMP_BUCKET="${TEMP_BUCKET:-giga-dataflow-temp-quoriagigamartproject}"

# Composer image version
IMAGE_VERSION="${IMAGE_VERSION:-composer-3-airflow-2.10.5}"

EMAIL_ON_FAILURE="${EMAIL_ON_FAILURE:-lmkppadmasanka@gmail.com}"

echo ">>> Project:          ${PROJECT_ID}"
echo ">>> Region:           ${REGION}"
echo ">>> Composer env:     ${ENV_NAME}"
echo ">>> Composer SA:      ${COMPOSER_SA}"
echo ">>> Dataflow SA:      ${DATAFLOW_SA}"
echo ">>> RAW bucket:       ${RAW_BUCKET}"
echo ">>> STAGING bucket:   ${STAGING_BUCKET}"
echo ">>> TEMP bucket:      ${TEMP_BUCKET}"
echo ">>> Image version:    ${IMAGE_VERSION}"

gcloud config set project "${PROJECT_ID}"

# If environment already exists, don't try to recreate it
if gcloud composer environments describe "${ENV_NAME}" --location="${REGION}" >/dev/null 2>&1; then
  echo ">>> Composer environment ${ENV_NAME} already exists in ${REGION}, skipping creation."
  exit 0
fi

gcloud composer environments create "${ENV_NAME}" \
  --location="${REGION}" \
  --service-account="${COMPOSER_SA}" \
  --image-version="${IMAGE_VERSION}" \
  --labels="env=dev,owner=gigamart" \
  --env-variables="GCP_REGION=${REGION},RAW_BUCKET=${RAW_BUCKET},STAGING_BUCKET=${STAGING_BUCKET},TEMP_BUCKET=${TEMP_BUCKET},DATAFLOW_WORKER_SA=${DATAFLOW_SA},EMAIL_ON_FAILURE=${EMAIL_ON_FAILURE}"

echo ">>> Composer environment ${ENV_NAME} creation requested."
echo "    Inspect with:"
echo "    gcloud composer environments describe ${ENV_NAME} --location ${REGION}"