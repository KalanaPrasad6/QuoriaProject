set -euo pipefail

PROJECT_ID="${PROJECT_ID:-quoriagigamartproject}"

gcloud config set project "$PROJECT_ID"

gcloud services enable \
  compute.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  composer.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  iam.googleapis.com \
  cloudresourcemanager.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  serviceusage.googleapis.com \
  cloudresourcemanager.googleapis.com
