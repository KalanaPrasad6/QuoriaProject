import argparse, logging
from typing import List, Dict, Any
import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.io.gcp.pubsub import ReadFromPubSub

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.schemas import RAW_POS_SCHEMA
from src.common.transforms import pos_json_line_to_row
from src.common.utils import load_config
from src.common.deduplication import generate_record_hash, get_deduplication_keys

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def parse_pubsub_message(message: bytes) -> Dict[str, Any]:
    # Parse Pub/Sub message and convert to row.
    try:
        message_str = message.decode("utf-8")
        payload = json.loads(message_str) 

        row = pos_json_line_to_row(message_str, "pubsub://pos-transactions")

        key_fields = get_deduplication_keys("raw_pos_transactions")
        if key_fields:
            row["record_hash"] = generate_record_hash(row, key_fields)

        return row
    except Exception as e: 
        LOGGER.error("Failed to parse Pub/Sub message: %s", e)
        return None


def add_timestamp(element: Dict[str, Any]) -> TimestampedValue:
    # Add timestamp to element for windowing.
    event_ts = element.get("event_ts")
    if event_ts:
        try:
            if isinstance(event_ts, str):
                dt = datetime.fromisoformat(event_ts.replace("Z", "+00:00"))
            else:
                dt = event_ts
            return TimestampedValue(element, dt.timestamp())
        except Exception as e: 
            LOGGER.warning("Failed to parse event_ts timestamp: %s", e)

    ingestion_ts = element.get("ingestion_ts")
    if ingestion_ts:
        try:
            if isinstance(ingestion_ts, str):
                dt = datetime.fromisoformat(ingestion_ts.replace("Z", "+00:00"))
            else:
                dt = ingestion_ts
            return TimestampedValue(element, dt.timestamp())
        except Exception:
            pass

    return TimestampedValue(element, datetime.now(timezone.utc).timestamp())


def run(argv: List[str] = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_subscription", required=True)
    parser.add_argument("--output_table", required=False)
    parser.add_argument("--window_size", default=60, type=int)
    parser.add_argument("--allowed_lateness", default=3600, type=int)
    known_args, pipeline_args = parser.parse_known_args(argv)

    config = load_config()
    project_id = config["project_id"]
    region = config["region"]

    output_table = (
        known_args.output_table
        or f"{project_id}:{config['bq']['raw_dataset']}.raw_pos_transactions"
    )

    options = PipelineOptions(pipeline_args)
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True
    standard_options = options.view_as(StandardOptions)
    standard_options.streaming = True 

    if (standard_options.runner or "").lower() == "dataflowrunner":
        if not getattr(setup_options, "setup_file", None):
            setup_options.setup_file = "/dataflow/setup.py"

    LOGGER.info("POS streaming: %s -> %s", known_args.input_subscription, output_table)
    LOGGER.info(
        "Window size: %d seconds, Allowed lateness: %d seconds",
        known_args.window_size,
        known_args.allowed_lateness,
    )

    with beam.Pipeline(options=options) as p:
        messages = p | "ReadFromPubSub" >> ReadFromPubSub(
            subscription=known_args.input_subscription
        )

        parsed = (
            messages
            | "ParseMessages" >> beam.Map(parse_pubsub_message)
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        windowed = (
            parsed
            | "AddTimestamp" >> beam.Map(add_timestamp)
            | "Window"
            >> beam.WindowInto(
                FixedWindows(known_args.window_size),
                allowed_lateness=known_args.allowed_lateness,
                accumulation_mode=beam.transforms.window.AccumulationMode.ACCUMULATING,
            )
        )

        _ = windowed | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            output_table,
            schema=RAW_POS_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
        )


if __name__ == "__main__":
    run()