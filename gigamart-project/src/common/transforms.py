import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any

LOGGER = logging.getLogger(__name__)


def parse_timestamp(ts: str):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        LOGGER.exception("Failed to parse timestamp: %s", ts)
        return None


def to_date(dt):
    return dt.date() if dt else None


def safe_float(value):
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception: 
        LOGGER.warning("Failed to convert %r to float", value)
        return None


def safe_int(value):
    try:
        if value in (None, ""):
            return None
        return int(value)
    except Exception:
        LOGGER.warning("Failed to convert %r to int", value)
        return None


def pos_json_line_to_row(line: str, source_file: str) -> Dict[str, Any]:
    """
    Transform a single POS JSON line into a normalized row.

    Adds basic DQ fields:
      - dq_status: 'VALID' or 'INVALID'
      - dq_errors: error description for invalid rows
    """
    ingestion_ts = datetime.now(timezone.utc)
    try:
        payload = json.loads(line)
    except Exception:
        LOGGER.exception("Failed to parse POS JSON line")
        return {
            "transaction_id": None,
            "store_id": None,
            "customer_id": None,
            "amount": None,
            "currency": None,
            "event_ts": None,
            "event_date": None,
            "ingestion_ts": ingestion_ts.isoformat(),
            "source_file": source_file,
            "raw_payload": line,
            "dq_status": "INVALID",
            "dq_errors": "JSON_PARSE_ERROR",
        }

    ts = parse_timestamp(payload.get("timestamp"))
    return {
        "transaction_id": payload.get("transaction_id"),
        "store_id": payload.get("store_id"),
        "customer_id": payload.get("customer_id"),
        "amount": safe_float(payload.get("amount")),
        "currency": payload.get("currency"),
        "event_ts": ts.isoformat() if ts else None,
        "event_date": to_date(ts),
        "ingestion_ts": ingestion_ts.isoformat(),
        "source_file": source_file,
        "raw_payload": json.dumps(payload),
        "dq_status": "VALID",
        "dq_errors": None,
    }


def ecommerce_json_line_to_row(line: str, source_file: str) -> Dict[str, Any]:
    """
    Transform a single E-commerce JSON line into a normalized row.

    Adds basic DQ fields similar to POS.
    """
    ingestion_ts = datetime.now(timezone.utc)
    try:
        payload = json.loads(line)
    except Exception:
        LOGGER.exception("Failed to parse E-com JSON line")
        return {
            "order_id": None,
            "customer_id": None,
            "store_id": None,
            "total_amount": None,
            "payment_type": None,
            "order_ts": None,
            "order_date": None,
            "ingestion_ts": ingestion_ts.isoformat(),
            "source_file": source_file,
            "raw_payload": line,
            "dq_status": "INVALID",
            "dq_errors": "JSON_PARSE_ERROR",
        }

    ts = parse_timestamp(payload.get("timestamp"))
    return {
        "order_id": payload.get("order_id"),
        "customer_id": payload.get("customer_id"),
        "store_id": payload.get("store_id"),
        "total_amount": safe_float(payload.get("total_amount")),
        "payment_type": payload.get("payment_type"),
        "order_ts": ts.isoformat() if ts else None,
        "order_date": to_date(ts),
        "ingestion_ts": ingestion_ts.isoformat(),
        "source_file": source_file,
        "raw_payload": json.dumps(payload),
        "dq_status": "VALID",
        "dq_errors": None,
    }


def crm_csv_line_to_row(line: str, source_file: str) -> Dict[str, Any]:
    """
    Transform a single CRM CSV line into a normalized row.

    Handles both legacy and extended formats.
    Adds basic DQ fields for parsing errors.
    """
    import csv

    ingestion_ts = datetime.now(timezone.utc)
    try:
        reader = csv.reader([line])
        cols = next(reader)
    except Exception: 
        LOGGER.exception("Failed to parse CRM CSV line")
        return {
            "customer_id": None,
            "first_name": None,
            "last_name": None,
            "email": None,
            "loyalty_status": None,
            "city": None,
            "country": None,
            "updated_at": None,
            "ingestion_ts": ingestion_ts.isoformat(),
            "source_file": source_file,
            "raw_payload": line,
            "dq_status": "INVALID",
            "dq_errors": "CSV_PARSE_ERROR",
        }

    customer_id = cols[0] if len(cols) > 0 else None
    first_name = cols[1] if len(cols) > 1 else None
    last_name = cols[2] if len(cols) > 2 else None
    email = cols[3] if len(cols) > 3 else None
    loyalty_status = cols[4] if len(cols) > 4 else None
    city = cols[5] if len(cols) > 5 else None
    country = cols[6] if len(cols) > 6 else None

    updated_at_str = None
    if len(cols) == 8:
        updated_at_str = cols[7]
    elif len(cols) >= 9:
        updated_at_str = cols[8]

    ts = parse_timestamp(updated_at_str) if updated_at_str else None

    return {
        "customer_id": customer_id,
        "first_name": first_name,
        "last_name": last_name,
        "email": email,
        "loyalty_status": loyalty_status,
        "city": city,
        "country": country,
        "updated_at": ts.isoformat() if ts else None,
        "ingestion_ts": ingestion_ts.isoformat(),
        "source_file": source_file,
        "raw_payload": line,
        "dq_status": "VALID",
        "dq_errors": None,
    }


def inventory_csv_line_to_row(line: str, source_file: str) -> Dict[str, Any]:
    """
    Transform a single inventory CSV line into a normalized row.

    Adds basic DQ fields for parsing errors.
    """
    import csv

    ingestion_ts = datetime.now(timezone.utc)
    try:
        reader = csv.reader([line])
        cols = next(reader)
    except Exception:
        LOGGER.exception("Failed to parse inventory CSV line")
        return {
            "product_id": None,
            "store_id": None,
            "product_name": None,
            "quantity": None,
            "price": None,
            "last_updated": None,
            "ingestion_ts": ingestion_ts.isoformat(),
            "source_file": source_file,
            "raw_payload": line,
            "dq_status": "INVALID",
            "dq_errors": "CSV_PARSE_ERROR",
        }

    product_id = cols[0] if len(cols) > 0 else None
    store_id = cols[1] if len(cols) > 1 else None
    product_name = cols[2] if len(cols) > 2 else None
    quantity = safe_int(cols[3]) if len(cols) > 3 else None
    price = safe_float(cols[4]) if len(cols) > 4 else None
    last_updated_str = cols[5] if len(cols) > 5 else None
    ts = parse_timestamp(last_updated_str) if last_updated_str else None

    return {
        "product_id": product_id,
        "store_id": store_id,
        "product_name": product_name,
        "quantity": quantity,
        "price": price,
        "last_updated": ts.isoformat() if ts else None,
        "ingestion_ts": ingestion_ts.isoformat(),
        "source_file": source_file,
        "raw_payload": line,
        "dq_status": "VALID",
        "dq_errors": None,
    }