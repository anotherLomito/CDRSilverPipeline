"""PySpark implementation for building the CDR Silver layer."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


EXPECTED_COLUMNS = [
    "call_id",
    "customer_id",
    "call_start",
    "call_end",
    "duration_seconds",
    "call_type",
    "origin_country",
    "destination_country",
    "cost_usd",
    "ingestion_date",
]

CRITICAL_COLUMNS = [
    "call_id",
    "customer_id",
    "call_start",
    "call_end",
    "duration_seconds",
    "call_type",
    "origin_country",
    "destination_country",
    "cost_usd",
]
OUTLIER_COLUMNS = ["duration_seconds", "cost_usd"]

SILVER_DETAIL_PATH = Path("silver/cdr_calls")
CUSTOMER_AGGREGATE_PATH = Path("silver/customer_aggregates")
SILVER_DETAIL_CSV_PATH = Path("csv/cdr_calls")
CUSTOMER_AGGREGATE_CSV_PATH = Path("csv/customer_aggregates")
REPORT_PATH = Path("reports/data_quality_report.json")


def _blank_to_null(column: str) -> F.Column:
    value = F.trim(F.col(column))
    return F.when(value == "", F.lit(None)).otherwise(value)


def _records_by_file(raw: DataFrame) -> dict[str, int]:
    rows = raw.groupBy("source_file").count().collect()
    return {row["source_file"]: int(row["count"]) for row in rows}


def _critical_null_percentages(raw: DataFrame, raw_count: int) -> dict[str, dict[str, Any]]:
    aggregations = [
        F.sum(
            F.when(
                F.col(column).isNull() | (F.trim(F.col(column)) == ""),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias(column)
        for column in CRITICAL_COLUMNS
    ]
    row = raw.agg(*aggregations).first().asDict()

    return {
        column: {
            "null_count": int(row[column] or 0),
            "null_percentage": round(((row[column] or 0) / raw_count) * 100, 2)
            if raw_count
            else 0.0,
        }
        for column in CRITICAL_COLUMNS
    }


def _quintile_outlier_report(
    silver_detail: DataFrame,
    columns: list[str],
) -> dict[str, dict[str, Any]]:
    report = {}

    for column in columns:
        non_null = silver_detail.filter(F.col(column).isNotNull())
        non_null_count = non_null.count()

        if non_null_count == 0:
            report[column] = {
                "quintiles": {
                    "p20": None,
                    "p40": None,
                    "p60": None,
                    "p80": None,
                },
                "lower_tail_count": 0,
                "upper_tail_count": 0,
                "outlier_candidate_count": 0,
            }
            continue

        p20, p40, p60, p80 = non_null.approxQuantile(
            column,
            [0.2, 0.4, 0.6, 0.8],
            0.0,
        )
        lower_tail_count = non_null.filter(F.col(column) < p20).count()
        upper_tail_count = non_null.filter(F.col(column) > p80).count()

        report[column] = {
            "quintiles": {
                "p20": round(p20, 6),
                "p40": round(p40, 6),
                "p60": round(p60, 6),
                "p80": round(p80, 6),
            },
            "lower_tail_count": lower_tail_count,
            "upper_tail_count": upper_tail_count,
            "outlier_candidate_count": lower_tail_count + upper_tail_count,
            "method": "values below p20 or above p80 are marked as outlier candidates",
        }

    return report


@dataclass(frozen=True)
class PipelineResult:
    """Container returned by the pipeline for tests and orchestration."""

    silver_detail: DataFrame
    customer_aggregates: DataFrame
    report: dict[str, Any]


def create_spark_session(
    app_name: str = "CDR Silver Pipeline",
    master: str = "local[*]",
) -> SparkSession:
    """Create a SparkSession with deterministic local defaults."""

    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def discover_csv_files(input_path: Path | str) -> list[Path]:
    """Return sorted CSV files from an input directory or a single CSV path."""

    path = Path(input_path)
    if path.is_file():
        return [path]

    files = sorted(path.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {path}")

    return files


def inspect_csv_files(input_path: Path | str) -> dict[str, dict[str, Any]]:
    """Inspect headers before Spark reads the files."""

    report: dict[str, dict[str, Any]] = {}
    for file_path in discover_csv_files(input_path):
        with file_path.open(newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            columns = next(reader, [])

        report[file_path.name] = {
            "columns": columns,
            "unexpected_columns": [
                column for column in columns if column not in EXPECTED_COLUMNS
            ],
            "missing_columns": [
                column for column in EXPECTED_COLUMNS if column not in columns
            ],
        }

    return report


def read_raw_cdrs(
    spark: SparkSession,
    input_path: Path | str,
) -> tuple[DataFrame, dict[str, dict[str, Any]]]:
    """Read all CSVs and enforce the expected raw schema.

    Each file is read independently so extra columns can be ignored safely and
    missing expected columns can be represented as nulls.
    """

    files = discover_csv_files(input_path)
    header_report = inspect_csv_files(input_path)
    frames: list[DataFrame] = []

    for file_path in files:
        frame = (
            spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .csv(str(file_path))
        )

        for column in EXPECTED_COLUMNS:
            if column not in frame.columns:
                frame = frame.withColumn(column, F.lit(None).cast(T.StringType()))

        frame = frame.select(
            *[F.col(column).cast(T.StringType()).alias(column) for column in EXPECTED_COLUMNS]
        ).withColumn("source_file", F.lit(file_path.name))

        frames.append(frame)

    raw = frames[0]
    for frame in frames[1:]:
        raw = raw.unionByName(frame)

    return raw, header_report


def normalize_records(raw: DataFrame) -> DataFrame:
    """Normalize strings and convert data types."""

    return raw.select(
        _blank_to_null("call_id").alias("call_id"),
        _blank_to_null("customer_id").alias("customer_id"),
        F.to_timestamp(
            _blank_to_null("call_start"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ).alias("call_start"),
        F.to_timestamp(
            _blank_to_null("call_end"),
            "yyyy-MM-dd'T'HH:mm:ss",
        ).alias("call_end"),
        _blank_to_null("duration_seconds").cast(T.LongType()).alias("duration_seconds"),
        F.upper(_blank_to_null("call_type")).alias("call_type"),
        F.upper(_blank_to_null("origin_country")).alias("origin_country"),
        F.upper(_blank_to_null("destination_country")).alias("destination_country"),
        _blank_to_null("cost_usd").cast(T.DoubleType()).alias("cost_usd"),
        F.to_date(
            _blank_to_null("ingestion_date"),
            "yyyy-MM-dd"
        ).alias("ingestion_date"),
        F.col("source_file"),
    )


def add_record_hash(frame: DataFrame) -> DataFrame:
    """Add an idempotency key based on canonical normalized values."""

    hash_columns = [
        "call_id",
        "customer_id",
        "call_start",
        "call_end",
        "duration_seconds",
        "call_type",
        "origin_country",
        "destination_country",
        "cost_usd",
        "ingestion_date",
    ]
    canonical_values = [
        F.coalesce(F.col(column).cast(T.StringType()), F.lit("__NULL__"))
        for column in hash_columns
    ]

    return frame.withColumn(
        "record_hash",
        F.sha2(F.to_json(F.array(*canonical_values)), 256),
    )


def build_silver_detail(normalized: DataFrame) -> DataFrame:
    """Remove invalid durations, add derived columns, hash, and deduplicate."""

    valid_duration = normalized.filter(
        F.col("duration_seconds").isNull() | (F.col("duration_seconds") >= 0)
    )

    enriched = (
        valid_duration.withColumn(
            "customer_id_is_null",
            F.col("customer_id").isNull(),
        )
        .withColumn(
            "call_duration_minutes",
            F.col("duration_seconds").cast(T.DoubleType()) / F.lit(60.0),
        )
        .withColumn(
            "is_international_call",
            F.when(
                F.col("origin_country").isNull()
                | F.col("destination_country").isNull(),
                F.lit(None).cast(T.BooleanType()),
            ).otherwise(F.col("origin_country") != F.col("destination_country")),
        )
    )

    with_hash = add_record_hash(enriched)
    window = Window.partitionBy("record_hash").orderBy(F.col("source_file").asc())

    return (
        with_hash.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
        .select(
            "record_hash",
            "source_file",
            "call_id",
            "customer_id",
            "customer_id_is_null",
            "call_start",
            "call_end",
            "duration_seconds",
            "call_duration_minutes",
            "call_type",
            "origin_country",
            "destination_country",
            "is_international_call",
            "cost_usd",
            "ingestion_date",
        )
    )


def build_customer_aggregates(silver_detail: DataFrame) -> DataFrame:
    """Build the customer-level analytical table."""

    return (
        silver_detail.filter(~F.col("customer_id_is_null"))
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_calls"),
            F.round(F.sum("call_duration_minutes"), 2).alias(
                "total_duration_minutes"
            ),
            F.round(F.sum("cost_usd"), 2).alias("total_cost"),
        )
        .orderBy("customer_id")
    )


def build_quality_report(
    input_path: Path | str,
    header_report: dict[str, dict[str, Any]],
    raw: DataFrame,
    normalized: DataFrame,
    silver_detail: DataFrame,
    customer_aggregates: DataFrame,
) -> dict[str, Any]:
    """Compute quality checks and output metadata."""

    raw_count = raw.count()
    valid_duration_count = normalized.filter(
        F.col("duration_seconds").isNull() | (F.col("duration_seconds") >= 0)
    ).count()
    negative_duration_count = normalized.filter(F.col("duration_seconds") < 0).count()
    silver_count = silver_detail.count()
    duplicate_count = valid_duration_count - silver_count
    null_customer_silver_count = silver_detail.filter(
        F.col("customer_id_is_null")
    ).count()
    aggregate_input_count = silver_detail.filter(~F.col("customer_id_is_null")).count()

    return {
        "input": {
            "path": str(input_path),
            "files": header_report,
            "records_by_file": _records_by_file(raw),
        },
        "quality_checks": {
            "raw_record_count": raw_count,
            "critical_nulls": _critical_null_percentages(raw, raw_count),
            "discarded_records": {
                "negative_duration": negative_duration_count,
                "total": negative_duration_count,
            },
            "duplicates_removed": duplicate_count,
            "customer_id_nulls_in_silver": null_customer_silver_count,
            "quintile_outlier_candidates": _quintile_outlier_report(
                silver_detail,
                OUTLIER_COLUMNS,
            ),
        },
        "outputs": {
            "silver_detail_rows": silver_count,
            "customer_aggregate_rows": customer_aggregates.count(),
            "customer_aggregate_input_rows": aggregate_input_count,
            "silver_detail_path": str(SILVER_DETAIL_PATH),
            "customer_aggregate_path": str(CUSTOMER_AGGREGATE_PATH),
            "silver_detail_csv_path": str(SILVER_DETAIL_CSV_PATH),
            "customer_aggregate_csv_path": str(CUSTOMER_AGGREGATE_CSV_PATH),
        },
        "idempotency": {
            "strategy": "canonical_sha256_record_hash",
            "key_column": "record_hash",
            "write_mode": "overwrite",
        },
    }


def run_pipeline(
    spark: SparkSession,
    input_path: Path | str,
    output_path: Path | str = "outputs",
    write: bool = True,
) -> PipelineResult:
    """Run the full CDR Silver pipeline."""

    raw, header_report = read_raw_cdrs(spark, input_path)
    normalized = normalize_records(raw)
    silver_detail = build_silver_detail(normalized)
    customer_aggregates = build_customer_aggregates(silver_detail)
    report = build_quality_report(
        input_path=input_path,
        header_report=header_report,
        raw=raw,
        normalized=normalized,
        silver_detail=silver_detail,
        customer_aggregates=customer_aggregates,
    )

    if write:
        write_outputs(
            output_path=Path(output_path),
            silver_detail=silver_detail,
            customer_aggregates=customer_aggregates,
            report=report,
        )

    return PipelineResult(
        silver_detail=silver_detail,
        customer_aggregates=customer_aggregates,
        report=report,
    )


def write_outputs(
    output_path: Path,
    silver_detail: DataFrame,
    customer_aggregates: DataFrame,
    report: dict[str, Any],
) -> None:
    """Write Silver datasets and the quality report."""

    (output_path / SILVER_DETAIL_PATH).parent.mkdir(parents=True, exist_ok=True)
    (output_path / REPORT_PATH).parent.mkdir(parents=True, exist_ok=True)

    silver_detail.write.mode("overwrite").parquet(
        str(output_path / SILVER_DETAIL_PATH)
    )
    customer_aggregates.write.mode("overwrite").parquet(
        str(output_path / CUSTOMER_AGGREGATE_PATH)
    )
    silver_detail.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(output_path / SILVER_DETAIL_CSV_PATH)
    )
    customer_aggregates.coalesce(1).write.mode("overwrite").option("header", True).csv(
        str(output_path / CUSTOMER_AGGREGATE_CSV_PATH)
    )

    with (output_path / REPORT_PATH).open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")
