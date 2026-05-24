from pyspark.sql import functions as F

from pipeline import (
    build_customer_aggregates,
    build_quality_report,
    build_silver_detail,
    inspect_csv_files,
    normalize_records,
    read_raw_cdrs,
    run_pipeline,
)
from schema import RAW_COLUMNS


def test_read_raw_cdrs_uses_consistent_schema_and_ignores_extra_columns(
    spark, raw_data_path
):
    raw_df, metadata = read_raw_cdrs(spark, raw_data_path)

    assert raw_df.count() == 43
    assert raw_df.columns == RAW_COLUMNS + ["source_file"]
    assert len(metadata["cdr_batch_20260303_extra_column.csv"]["unexpected_columns"]) != 0


def test_silver_transformations_and_quality_counts(spark, raw_data_path):
    raw_df, _ = read_raw_cdrs(spark, raw_data_path)
    normalized_df = normalize_records(raw_df)
    silver_df = build_silver_detail(normalized_df)

    valid_duration_count = normalized_df.filter(
        F.col("duration_seconds").isNull() | (F.col("duration_seconds") >= 0)
    ).count()
    assert normalized_df.filter(F.col("duration_seconds") < 0).count() == 12
    assert valid_duration_count - silver_df.count() == 1
    assert silver_df.count() == 30
    assert silver_df.filter(F.col("customer_id_is_null")).count() == 2
    assert silver_df.filter(F.col("duration_seconds") < 0).count() == 0
    assert silver_df.filter(F.col("call_type") == "VoIcE").count() == 0
    assert silver_df.filter(F.col("call_type") == "VOICE").count() > 0
    assert silver_df.select("record_hash").distinct().count() == silver_df.count()


def test_business_columns_are_calculated(spark, raw_data_path):
    raw_df, _ = read_raw_cdrs(spark, raw_data_path)
    silver_df = build_silver_detail(normalize_records(raw_df))

    row = silver_df.filter(F.col("call_id") == "C026").select(
        "duration_seconds",
        "call_duration_minutes",
        "origin_country",
        "destination_country",
        "is_international_call",
    ).first()

    assert row.duration_seconds == 478
    assert round(row.call_duration_minutes, 6) == round(478 / 60, 6)
    assert row.origin_country == "CL"
    assert row.destination_country == "CL"
    assert row.is_international_call is False


def test_customer_aggregate_excludes_null_customer_id(spark, raw_data_path):
    raw_df, _ = read_raw_cdrs(spark, raw_data_path)
    silver_df = build_silver_detail(normalize_records(raw_df))
    aggregate_df = build_customer_aggregates(silver_df)

    assert silver_df.count() == 30
    assert silver_df.filter(~F.col("customer_id_is_null")).count() == 28
    assert aggregate_df.filter(F.col("customer_id").isNull()).count() == 0
    assert aggregate_df.agg(F.sum("total_calls")).first()[0] == 28


def test_quality_report_contains_required_checks(spark, raw_data_path):
    raw_df, metadata = read_raw_cdrs(spark, raw_data_path)
    normalized_df = normalize_records(raw_df)
    silver_df = build_silver_detail(normalized_df)
    aggregate_df = build_customer_aggregates(silver_df)
    assert metadata == inspect_csv_files(raw_data_path)
    report = build_quality_report(
        input_path=raw_data_path,
        header_report=metadata,
        raw=raw_df,
        normalized=normalized_df,
        silver_detail=silver_df,
        customer_aggregates=aggregate_df,
    )

    assert report["input"]["records_by_file"] == {
        "cdr_batch_20260301.csv": 15,
        "cdr_batch_20260302.csv": 18,
        "cdr_batch_20260303_extra_column.csv": 10,
    }
    assert report["quality_checks"]["critical_nulls"]["customer_id"] == {
        "null_count": 4,
        "null_percentage": 9.3,
    }
    assert report["quality_checks"]["discarded_records"]["negative_duration"] == 12
    assert report["quality_checks"]["duplicates_removed"] == 1
    assert report["outputs"]["customer_aggregate_input_rows"] == 28
    assert report["outputs"]["silver_detail_csv_path"] == "csv/cdr_calls"
    assert report["outputs"]["customer_aggregate_csv_path"] == "csv/customer_aggregates"

    outliers = report["quality_checks"]["quintile_outlier_candidates"]
    assert set(outliers) == {"duration_seconds", "cost_usd"}
    for column_report in outliers.values():
        quintiles = column_report["quintiles"]
        assert quintiles["p20"] <= quintiles["p40"] <= quintiles["p60"] <= quintiles["p80"]
        assert column_report["outlier_candidate_count"] == (
            column_report["lower_tail_count"] + column_report["upper_tail_count"]
        )


def test_pipeline_writes_parquet_csv_and_report(spark, raw_data_path, tmp_path):
    run_pipeline(spark, raw_data_path, tmp_path, write=True)

    assert spark.read.parquet(str(tmp_path / "silver" / "cdr_calls")).count() == 30
    assert spark.read.parquet(
        str(tmp_path / "silver" / "customer_aggregates")
    ).agg(F.sum("total_calls")).first()[0] == 28
    assert spark.read.option("header", True).csv(
        str(tmp_path / "csv" / "cdr_calls")
    ).count() == 30
    assert spark.read.option("header", True).csv(
        str(tmp_path / "csv" / "customer_aggregates")
    ).agg(F.sum(F.col("total_calls").cast("int"))).first()[0] == 28
    assert (tmp_path / "reports" / "data_quality_report.json").exists()
