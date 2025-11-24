import logging
from typing import Dict, Any, List
import json

from google.cloud import bigquery

LOGGER = logging.getLogger(__name__)


def detect_schema_drift(
    client: bigquery.Client,
    table_ref: str,
    new_schema: List[Dict],
) -> Dict[str, Any]:
    
    # Detect schema drift by comparing new schema with existing table schema.

    try:
        table = client.get_table(table_ref)
        existing_fields = {field.name: field for field in table.schema}
        new_fields = {field["name"]: field for field in new_schema}

        added_fields: List[str] = []
        removed_fields: List[str] = []
        type_changes: List[Dict[str, str]] = []

        for field_name, field_def in new_fields.items():
            if field_name not in existing_fields:
                added_fields.append(field_name)
                LOGGER.info(
                    "New field detected: %s (%s)",
                    field_name,
                    field_def.get("type", "UNKNOWN"),
                )
            else:
                existing_type = existing_fields[field_name].field_type
                new_type = field_def.get("type", "").upper()
                if existing_type != new_type:
                    type_changes.append(
                        {
                            "field": field_name,
                            "old_type": existing_type,
                            "new_type": new_type,
                        }
                    )
                    LOGGER.warning(
                        "Type change detected for %s: %s -> %s",
                        field_name,
                        existing_type,
                        new_type,
                    )

        for field_name in existing_fields:
            if field_name not in new_fields:
                removed_fields.append(field_name)
                LOGGER.warning("Field removed from source: %s", field_name)

        return {
            "has_drift": bool(added_fields or removed_fields or type_changes),
            "added_fields": added_fields,
            "removed_fields": removed_fields,
            "type_changes": type_changes,
        }
    except Exception as exc:
        LOGGER.error("Error detecting schema drift: %s", exc)
        return {
            "has_drift": False,
            "error": str(exc),
        }


def merge_schemas(existing_schema: List[Dict], new_schema: List[Dict]) -> List[Dict]:
    """
    Merge schemas, adding new fields while preserving existing ones.
    New fields are added as NULLABLE to avoid breaking existing data.
    """
    existing_fields = {field["name"]: field for field in existing_schema}
    merged = existing_schema.copy()

    for new_field in new_schema:
        field_name = new_field["name"]
        if field_name not in existing_fields:
            # Make new fields NULLABLE to avoid breaking existing data
            new_field["mode"] = "NULLABLE"
            merged.append(new_field)
            LOGGER.info("Added new field to schema: %s", field_name)

    return merged


def handle_schema_drift(
    client: bigquery.Client,
    table_ref: str,
    new_schema: List[Dict],
    auto_update: bool = False,
) -> bool:
    """
    Handle schema drift by detecting and optionally updating the table schema.
    Returns True if schema was updated, False otherwise.
    """
    drift_info = detect_schema_drift(client, table_ref, new_schema)

    if not drift_info.get("has_drift", False):
        return False

    if not auto_update:
        LOGGER.warning(
            "Schema drift detected but auto_update is False. Drift info: %s",
            drift_info,
        )
        return False

    try:
        table = client.get_table(table_ref)
        existing_schema = [
            {"name": f.name, "type": f.field_type, "mode": f.mode}
            for f in table.schema
        ]
        merged_schema = merge_schemas(existing_schema, new_schema)

        # Convert to BigQuery Schema objects
        from google.cloud.bigquery import SchemaField

        new_bq_schema = []
        for field in merged_schema:
            new_bq_schema.append(
                SchemaField(
                    name=field["name"],
                    field_type=field["type"],
                    mode=field.get("mode", "NULLABLE"),
                )
            )

        table.schema = new_bq_schema
        client.update_table(table, ["schema"])
        LOGGER.info("Schema updated for table %s", table_ref)
        return True
    except Exception as exc:
        LOGGER.error("Failed to update schema: %s", exc)
        return False



CRM_BASE_FIELDS = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "loyalty_status",
    "city",
    "country",
    "membership_tier",
    "updated_at",
]


def normalize_crm_record(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a CRM record while being tolerant to new optional fields.

    - Ensures all base CRM fields exist.
    - Collects any additional/unknown fields into JSON in 'raw_extra_attributes'.

    """
    base = {field: row.get(field) for field in CRM_BASE_FIELDS}
    extra = {k: v for k, v in row.items() if k not in CRM_BASE_FIELDS}

    base["raw_extra_attributes"] = json.dumps(extra) if extra else None
    return base