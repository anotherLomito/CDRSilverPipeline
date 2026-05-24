from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_COLUMNS = [
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

RAW_SCHEMA = StructType(
    [
        StructField("call_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("call_start", StringType(), True),
        StructField("call_end", StringType(), True),
        StructField("duration_seconds", StringType(), True),
        StructField("call_type", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("destination_country", StringType(), True),
        StructField("cost_usd", StringType(), True),
        StructField("ingestion_date", StringType(), True),
    ]
)

SILVER_SCHEMA = StructType(
    [
        StructField("record_hash", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("call_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_id_is_null", BooleanType(), False),
        StructField("call_start", TimestampType(), True),
        StructField("call_end", TimestampType(), True),
        StructField("duration_seconds", LongType(), True),
        StructField("call_duration_minutes", DoubleType(), True),
        StructField("call_type", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("destination_country", StringType(), True),
        StructField("is_international_call", BooleanType(), True),
        StructField("cost_usd", DoubleType(), True),
        StructField("ingestion_date", DateType(), True),
    ]
)

CUSTOMER_AGGREGATE_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("total_calls", LongType(), False),
        StructField("total_duration_minutes", DoubleType(), True),
        StructField("total_cost", DoubleType(), True),
    ]
)
