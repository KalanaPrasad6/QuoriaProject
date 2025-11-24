import argparse
import logging
from typing import List, Tuple, Dict, Any

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.schemas import RAW_INVENTORY_SCHEMA
from src.common.transforms import inventory_csv_line_to_row
from src.common.utils import load_config
from src.common.deduplication import dedupe_by_id_and_timestamp

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def parse_with_error_handling(line: str, source_file: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    # Parse line and return (success, error) tuple
    try:
        row = inventory_csv_line_to_row(line, source_file)
        if row.get("product_id") is None or row.get("store_id") is None:
            return (
                None,
                {
                    "line": line,
                    "error": "Missing product_id or store_id",
                    "source_file": source_file,
                },
            )
        return (row, None)
    except Exception as e:
        LOGGER.warning("Failed to parse line: %s", str(e))
        return (
            None,
            {
                "line": line,
                "error": str(e),
                "source_file": source_file,
            },
        )


def run(argv: List[str] = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_pattern", required=False)
    parser.add_argument("--output_table", required=False)
    parser.add_argument("--dead_letter_table", required=False)
    known_args, pipeline_args = parser.parse_known_args(argv)

    config = load_config()
    project_id = config["project_id"]

    input_pattern = (
        known_args.input_pattern
        or f"gs://{config['gcs']['raw_bucket']}/{config['gcs']['inventory_prefix']}*.csv"
    )
    output_table = (
        known_args.output_table
        or f"{project_id}:{config['bq']['raw_dataset']}.raw_inventory_snapshots"
    )

    options = PipelineOptions(pipeline_args)
    setup_options = options.view_as(SetupOptions)
    setup_options.save_main_session = True
    standard_options = options.view_as(StandardOptions)

    if (standard_options.runner or "").lower() == "dataflowrunner":
        if not getattr(setup_options, "setup_file", None):
            setup_options.setup_file = "/dataflow/setup.py"

    LOGGER.info("Inventory batch: %s -> %s", input_pattern, output_table)
    LOGGER.info("Runner (from options): %s", standard_options.runner or "default")

    with beam.Pipeline(options=options) as p:
        lines = p | "ReadInventory" >> beam.io.ReadFromText(
            input_pattern,
            skip_header_lines=1,
        )

        parsed = lines | "ParseWithErrors" >> beam.Map(
            lambda line: parse_with_error_handling(line, input_pattern)
        )

        success_rows = (
            parsed
            | "FilterSuccess" >> beam.Filter(lambda x: x[0] is not None)
            | "ExtractSuccess" >> beam.Map(lambda x: x[0])
        )
        error_rows = (
            parsed
            | "FilterErrors" >> beam.Filter(lambda x: x[1] is not None)
            | "ExtractErrors" >> beam.Map(lambda x: x[1])
        )

        # Composite key product_id|store_id for dedup
        with_key = success_rows | "AddCompositeKey" >> beam.Map(
            lambda r: {**r, "_dedupe_id": f"{r.get('product_id', '')}|{r.get('store_id', '')}"}
        )

        deduped_with_key = dedupe_by_id_and_timestamp(
            with_key,
            id_field="_dedupe_id",
            ts_field="last_updated",
            label_prefix="InventoryBatch",
        )

        deduped_success = deduped_with_key | "DropCompositeKey" >> beam.Map(
            lambda r: {k: v for k, v in r.items() if k != "_dedupe_id"}
        )

        _ = deduped_success | "WriteBQ" >> beam.io.WriteToBigQuery(
            output_table,
            schema=RAW_INVENTORY_SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        _ = (
            error_rows
            | "CountErrors" >> beam.combiners.Count.Globally()
            | "LogErrors"
            >> beam.Map(
                lambda count: LOGGER.warning("Failed to process %d records", count)
                if count > 0
                else None
            )
        )


if __name__ == "__main__":
    run()
