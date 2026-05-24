"""CLI for running the CDR Silver Pipeline."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from pipeline import create_spark_session, run_pipeline
except ModuleNotFoundError:
    from .pipeline import create_spark_session, run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the CDR Silver datasets.")
    parser.add_argument(
        "--input",
        default="data",
        help="Directory containing raw CDR CSV files.",
    )
    parser.add_argument(
        "--output",
        default="outputs",
        help="Directory where Silver datasets and reports will be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    spark = create_spark_session()

    try:
        result = run_pipeline(
            spark=spark,
            input_path=input_path,
            output_path=output_path,
            write=True,
        )
        print(json.dumps(result.report, indent=2, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
