"""
Job: Source and Sink — Flight Time Data
Read flight data using two schema definition styles,
repartition, and write to different sinks.

Run:   python -m src.jobs.flight_source_sink
Input: data/input/flight-time.csv
Output:
    data/output/flight_repartitioned/  (5 equal partitions, JSON)
    data/output/flight_by_carrier/     (partitioned by OP_CARRIER + ORIGIN, JSON)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from src.utils import get_spark, get_logger

log = get_logger(__name__)

INPUT_PATH  = "data/input/flight-time.csv"
OUTPUT_DIR  = "data/output"

# Schema defined via StructType — explicit and type-safe
FLIGHT_SCHEMA_STRUCT = StructType([
    StructField("FL_DATE",           DateType(),    True),
    StructField("OP_CARRIER",        StringType(),  True),
    StructField("OP_CARRIER_FL_NUM", IntegerType(), True),
    StructField("ORIGIN",            StringType(),  True),
    StructField("ORIGIN_CITY_NAME",  StringType(),  True),
    StructField("DEST",              StringType(),  True),
    StructField("DEST_CITY_NAME",    StringType(),  True),
    StructField("CRS_DEP_TIME",      IntegerType(), True),
    StructField("DEP_TIME",          IntegerType(), True),
    StructField("WHEELS_ON",         IntegerType(), True),
    StructField("TAXI_IN",           IntegerType(), True),
    StructField("CRS_ARR_TIME",      IntegerType(), True),
    StructField("ARR_TIME",          IntegerType(), True),
    StructField("CANCELLED",         IntegerType(), True),
    StructField("DISTANCE",          IntegerType(), True),
])

# Schema defined via DDL string — shorter, readable, same result as StructType
FLIGHT_SCHEMA_DDL = """
    FL_DATE           DATE,
    OP_CARRIER        STRING,
    OP_CARRIER_FL_NUM INT,
    ORIGIN            STRING,
    ORIGIN_CITY_NAME  STRING,
    DEST              STRING,
    DEST_CITY_NAME    STRING,
    CRS_DEP_TIME      INT,
    DEP_TIME          INT,
    WHEELS_ON         INT,
    TAXI_IN           INT,
    CRS_ARR_TIME      INT,
    ARR_TIME          INT,
    CANCELLED         INT,
    DISTANCE          INT
"""


def read_with_struct_schema(spark: SparkSession) -> DataFrame:
    """
    Read CSV using StructType schema.
    FAILFAST mode throws an exception immediately on malformed rows —
    stricter than DROPMALFORMED but safer for catching data quality issues early.
    """
    log.info("Reading with StructType schema...")
    return (
        spark.read
             .format("csv")
             .option("header", "true")
             .schema(FLIGHT_SCHEMA_STRUCT)
             .option("mode", "FAILFAST")
             .option("dateFormat", "M/d/y")
             .load(INPUT_PATH)
    )


def read_with_ddl_schema(spark: SparkSession) -> DataFrame:
    """
    Read CSV using DDL string schema.
    DDL is more concise than StructType — preferred when schema is stable
    and doesn't need programmatic construction.
    """
    log.info("Reading with DDL string schema...")
    return (
        spark.read
             .format("csv")
             .option("header", "true")
             .schema(FLIGHT_SCHEMA_DDL)
             .option("mode", "FAILFAST")
             .option("dateFormat", "M/d/y")
             .load(INPUT_PATH)
    )


def show_partition_info(df: DataFrame, label: str) -> None:
    """Print partition count and row distribution across partitions."""
    log.info(f"{label} — partitions: {df.rdd.getNumPartitions()}")
    df.groupBy(F.spark_partition_id().alias("partition_id")).count().orderBy("partition_id").show()


def run() -> None:
    spark = get_spark("FlightSourceSink")
    log.header("Job: Source and Sink — Flight Time")

    # ── Read ────────────────────────────────────────────
    log.header("Schema by StructType")
    df_struct = read_with_struct_schema(spark)
    df_struct.show(5)

    log.header("Schema by DDL String")
    df_ddl = read_with_ddl_schema(spark)
    df_ddl.show(5)

    # Use DDL version for sink operations — same data, less verbose
    df = df_ddl

    # ── Repartition into 5 equal partitions ─────────────
    log.header("Repartition")
    show_partition_info(df, "Before")

    # repartition(n) shuffles data evenly across n partitions —
    # use when downstream processing benefits from parallelism
    partitioned_df = df.repartition(5)
    show_partition_info(partitioned_df, "After")

    partitioned_df.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", f"{OUTPUT_DIR}/flight_repartitioned") \
        .save()
    log.ok(f"Saved 5-partition JSON → {OUTPUT_DIR}/flight_repartitioned")

    # ── Partition by columns ─────────────────────────────
    # partitionBy creates subdirectories per unique value —
    # e.g. OP_CARRIER=DL/ORIGIN=BOS/part-*.json
    # maxRecordsPerFile prevents oversized partition files
    df.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", f"{OUTPUT_DIR}/flight_by_carrier") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("maxRecordsPerFile", 10000) \
        .save()
    log.ok(f"Saved column-partitioned JSON → {OUTPUT_DIR}/flight_by_carrier")

    log.ok("Source and Sink done!")
    spark.stop()


if __name__ == "__main__":
    run()