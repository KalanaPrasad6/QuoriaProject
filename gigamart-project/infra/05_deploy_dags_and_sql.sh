set -euo pipefail

PROJECT_ID="${PROJECT_ID:-quoriagigamartproject}"
REGION="${REGION:-asia-south1}"
ENV_NAME="${ENV_NAME:-gigamart-composer}"

gcloud config set project "${PROJECT_ID}"

DAGS_PREFIX="$(gcloud composer environments describe "${ENV_NAME}" \
  --location="${REGION}" \
  --format='value(config.dagGcsPrefix)')"

if [[ -z "${DAGS_PREFIX}" ]]; then
  echo "ERROR: Could not determine dagGcsPrefix for environment ${ENV_NAME}"
  exit 1
fi

echo ">>> DAGs prefix: ${DAGS_PREFIX}"

# Ensure trailing slash on DAGS_PREFIX
if [[ "${DAGS_PREFIX}" != */ ]]; then
  DAGS_PREFIX="${DAGS_PREFIX}/"
fi

SQL_PREFIX="${DAGS_PREFIX}sql/"

gsutil cp dags/gigamart_daily_batch_dag.py          "${DAGS_PREFIX}"
gsutil cp dags/gigamart_hourly_pos_freshness_dag.py "${DAGS_PREFIX}"
gsutil cp dags/gigamart_hourly_ecommerce_dag.py     "${DAGS_PREFIX}"
gsutil cp dags/gigamart_realtime_pos_dag.py         "${DAGS_PREFIX}"
gsutil cp dags/gigamart_monitoring_dag.py           "${DAGS_PREFIX}"

gsutil cp sql/00_create_datasets.sql         "${SQL_PREFIX}"
gsutil cp sql/01_create_raw_tables.sql       "${SQL_PREFIX}"
gsutil cp sql/02_create_curated_tables.sql   "${SQL_PREFIX}"
gsutil cp sql/03_sp_customer_dim_scd2.sql    "${SQL_PREFIX}"
gsutil cp sql/04_optimize_tables.sql         "${SQL_PREFIX}"
gsutil cp sql/05_deduplication_procedure.sql "${SQL_PREFIX}"
gsutil cp sql/06_dq_checks.sql               "${SQL_PREFIX}"
gsutil cp sql/07_late_arriving_data.sql      "${SQL_PREFIX}"
gsutil cp sql/08_monitoring_queries.sql      "${SQL_PREFIX}"

echo ">>> DAGs and SQL deployed."