import logging
from typing import Dict, Any, List, Optional
import hashlib
from datetime import datetime, timezone

LOGGER = logging.getLogger(__name__)

try:
    import apache_beam as beam 
except ImportError:
    beam = None


def generate_record_hash(record: Dict[str, Any], key_fields: List[str]) -> str:
    """
    Generate a hash for a record based on key fields.
    Used for deduplication.
    """
    key_values = []
    for field in key_fields:
        value = record.get(field)
        if value is not None:
            key_values.append(str(value))
        else:
            key_values.append("")

    key_string = "|".join(key_values)
    return hashlib.md5(key_string.encode(), usedforsecurity=False).hexdigest() 

def deduplicate_by_key(
    records: List[Dict[str, Any]],
    key_fields: List[str],
    keep: str = "latest",
) -> List[Dict[str, Any]]:
    """
    Deduplicate records by key fields (Python list-based).

    keep:
        - 'latest': keep record with latest ingestion_ts
        - 'first' : keep first occurrence
    """
    seen: Dict[str, Dict[str, Any]] = {}
    ingestion_field = "ingestion_ts"

    for record in records:
        record_hash = generate_record_hash(record, key_fields)

        if record_hash not in seen:
            seen[record_hash] = record
        else:
            existing = seen[record_hash]

            if keep == "latest":
                existing_ts = existing.get(ingestion_field)
                current_ts = record.get(ingestion_field)

                if existing_ts and current_ts:
                    try:
                        if isinstance(existing_ts, str):
                            existing_ts = datetime.fromisoformat(
                                str(existing_ts).replace("Z", "+00:00")
                            )
                        if isinstance(current_ts, str):
                            current_ts = datetime.fromisoformat(
                                str(current_ts).replace("Z", "+00:00")
                            )

                        if current_ts > existing_ts:
                            seen[record_hash] = record
                            LOGGER.debug(
                                "Replaced duplicate record (hash: %s...) with newer version",
                                record_hash[:8],
                            )
                    except Exception as exc:
                        LOGGER.warning(
                            "Error comparing timestamps: %s, keeping existing record",
                            exc,
                        )
                elif current_ts and not existing_ts:
                    seen[record_hash] = record

    return list(seen.values())


def get_deduplication_keys(table_name: str) -> List[str]:
    
    # Get deduplication key fields for a table.
    
    keys = {
        "raw_pos_transactions": ["transaction_id", "event_ts"],
        "raw_ecom_orders": ["order_id"],
        "raw_crm_customers": ["customer_id", "updated_at"],
        "raw_inventory_snapshots": ["product_id", "store_id", "last_updated"],
    }

    if "." in table_name:
        table_name = table_name.split(".")[-1]

    return keys.get(table_name, [])



def _parse_iso_ts(value: Any) -> Optional[datetime]:

    # Parse ISO/RFC3339 timestamps like '2025-10-20T10:00:00Z' or without tz.

    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            LOGGER.debug("Failed to parse timestamp %r", value, exc_info=True)
            return None

    return None


if beam is not None:
    class LatestRecordByTimestamp(beam.CombineFn):

        # Beam CombineFn that keeps only the latest record by a timestamp field.

        def __init__(self, ts_field: str):
            self._ts_field = ts_field

        def create_accumulator(self) -> Optional[Dict[str, Any]]:
            return None

        def add_input(
            self,
            acc: Optional[Dict[str, Any]],
            element: Dict[str, Any],
        ) -> Optional[Dict[str, Any]]:
            if acc is None:
                return element

            new_ts = _parse_iso_ts(element.get(self._ts_field))
            acc_ts = _parse_iso_ts(acc.get(self._ts_field))

            # If we can't parse timestamps, keep the first non-null.
            if acc_ts is None:
                return element if new_ts is not None else acc
            if new_ts is None:
                return acc

            return element if new_ts > acc_ts else acc

        def merge_accumulators(self, accs):
            current: Optional[Dict[str, Any]] = None
            for acc in accs:
                if acc is None:
                    continue
                current = self.add_input(current, acc)
            return current

        def extract_output(self, acc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            return acc

    def dedupe_by_id_and_timestamp(
        pcoll,
        id_field: str,
        ts_field: str,
        label_prefix: str = "Dedupe",
    ):
        """
        Beam helper to deduplicate a PCollection of dict rows.

        Keeps the latest record per id_field based on ts_field.

        """
        return (
            pcoll
            | f"{label_prefix}-KeyById"
            >> beam.Map(lambda row: (row.get(id_field), row))
            | f"{label_prefix}-LatestPerKey"
            >> beam.CombinePerKey(LatestRecordByTimestamp(ts_field))
            | f"{label_prefix}-DropKey"
            >> beam.Map(lambda kv: kv[1])
        )