"""
Job: Sales Aggregation
Read order data → calculate revenue → aggregate by category and region.

Run:    python -m src.jobs.sales_agg
Input:  data/input/orders.csv  (falls back to SAMPLE_DATA if no path given)
Output: data/output/sales_by_category.parquet, data/output/sales_by_region.parquet
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.utils import get_spark, get_logger, show_summary, save_parquet

log = get_logger(__name__)

# Explicit schema instead of letting Spark infer —
# schema inference on CSV forces Spark to scan the file twice
SCHEMA = StructType([
    StructField("order_id",  StringType(),  False),
    StructField("product",   StringType(),  True),
    StructField("category",  StringType(),  True),
    StructField("quantity",  IntegerType(), True),
    StructField("price",     DoubleType(),  True),
    StructField("region",    StringType(),  True),
])

# For demo/learning only — do not use in production
SAMPLE_DATA: list[Row] = [
    Row(order_id="O001", product="Laptop",  category="Electronics", quantity=2, price=1200.0, region="North"),
    Row(order_id="O002", product="Phone",   category="Electronics", quantity=5, price=800.0,  region="South"),
    Row(order_id="O003", product="Desk",    category="Furniture",   quantity=1, price=350.0,  region="North"),
    Row(order_id="O004", product="Chair",   category="Furniture",   quantity=4, price=150.0,  region="East"),
    Row(order_id="O005", product="Laptop",  category="Electronics", quantity=1, price=1200.0, region="East"),
    Row(order_id="O006", product="Monitor", category="Electronics", quantity=3, price=450.0,  region="South"),
    Row(order_id="O007", product="Desk",    category="Furniture",   quantity=2, price=350.0,  region="West"),
    Row(order_id="O008", product="Phone",   category="Electronics", quantity=2, price=800.0,  region="North"),
]

OUTPUT_DIR = "data/output"


def transform(df: DataFrame) -> DataFrame:
    """
    Add a revenue column = quantity * price.

    Kept as a separate function so it can be unit tested independently from I/O —
    no real SparkSession needed when testing transform logic.
    """
    return df.withColumn("revenue", F.col("quantity") * F.col("price"))


def aggregate_by_category(df: DataFrame) -> DataFrame:
    """
    Calculate total revenue and order count per category.

    round(..., 2) prevents floating point noise in displayed output —
    e.g. 1200.0 * 2 can produce 2399.9999 instead of 2400.0.
    """
    return (
        df.groupBy("category")
          .agg(
              F.count("order_id").alias("orders"),
              F.sum("quantity").alias("total_qty"),
              F.round(F.sum("revenue"), 2).alias("total_revenue"),
              F.round(F.avg("revenue"), 2).alias("avg_revenue"),
          )
          .orderBy(F.col("total_revenue").desc())
    )


def aggregate_by_region(df: DataFrame) -> DataFrame:
    """Calculate total revenue per geographic region."""
    return (
        df.groupBy("region")
          .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
          .orderBy(F.col("total_revenue").desc())
    )


def run(input_path: str | None = None) -> None:
    spark = get_spark("SalesAggregation")
    log.header("Job: Sales Aggregation")

    if input_path is None:
        log.warn("No input_path provided — using sample data")
        # pyspark-stubs does not yet support PySpark 4.x —
        # createDataFrame overloads are unresolvable until stubs are updated
        df: DataFrame = spark.createDataFrame(SAMPLE_DATA, SCHEMA)  # type: ignore[arg-type]
    else:
        df = spark.read.schema(SCHEMA).csv(input_path, header=True)

    df = transform(df)
    show_summary(df, "Raw + Revenue")

    log.header("By Category")
    by_category = aggregate_by_category(df)
    by_category.show()
    save_parquet(by_category, f"{OUTPUT_DIR}/sales_by_category.parquet")

    log.header("By Region")
    by_region = aggregate_by_region(df)
    by_region.show()
    save_parquet(by_region, f"{OUTPUT_DIR}/sales_by_region.parquet")

    log.ok("Sales Aggregation done!")
    spark.stop()


if __name__ == "__main__":
    run()
