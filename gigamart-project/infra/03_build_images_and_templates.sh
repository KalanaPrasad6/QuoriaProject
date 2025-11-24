set -euo pipefail

PROJECT_ID="${PROJECT_ID:-quoriagigamartproject}"
REGION="${REGION:-asia-south1}"
TEMP_BUCKET="${TEMP_BUCKET:-giga-dataflow-temp-quoriagigamartproject}"
REPO="${REPO:-dataflow-images}"

gcloud config set project "${PROJECT_ID}" >/dev/null

gcloud artifacts repositories create "${REPO}" \
  --repository-format=docker \
  --location="${REGION}" \
  --description="GigaMart Dataflow Flex Template images" 2>/dev/null || echo "Repo ${REPO} already exists"

gcloud auth configure-docker "${REGION}-docker.pkg.dev" -q

# 1) Build base Dataflow image with all code + deps + PYTHONPATH=/dataflow

BASE_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/gigamart-dataflow:latest"

docker build . \
  -f infra/docker/gigamart-dataflow.Dockerfile \
  -t "${BASE_IMAGE}"

docker push "${BASE_IMAGE}"

# 2) Helper: build/push image and build Flex template JSON

build_image_and_template() {
  local name="$1"     
  local dockerfile="$2" 
  local metadata_file="$3"

  local image="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/gigamart-${name}:latest"
  local template_path="gs://${TEMP_BUCKET}/flex-templates/${name}.json"

  docker build . \
    -f "${dockerfile}" \
    -t "${image}"

  docker push "${image}"

  gcloud dataflow flex-template build "${template_path}" \
    --image="${image}" \
    --sdk-language="PYTHON" \
    --metadata-file="${metadata_file}"
}

build_image_and_template \
  "ecommerce_batch" \
  "infra/docker/gigamart-ecommerce_batch.Dockerfile" \
  "metadata/ecommerce_batch_metadata.json"

build_image_and_template \
  "crm_batch" \
  "infra/docker/gigamart-crm_batch.Dockerfile" \
  "metadata/crm_batch_metadata.json"

build_image_and_template \
  "inventory_batch" \
  "infra/docker/gigamart-inventory_batch.Dockerfile" \
  "metadata/inventory_batch_metadata.json"

build_image_and_template \
  "pos_batch" \
  "infra/docker/gigamart-pos_batch.Dockerfile" \
  "metadata/pos_batch_metadata.json"

build_image_and_template \
  "pos_streaming" \
  "infra/docker/gigamart-pos_streaming.Dockerfile" \
  "metadata/pos_streaming_metadata.json"

echo "    Templates stored under: gs://${TEMP_BUCKET}/flex-templates/"