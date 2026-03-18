"""
Job: Aggregation Demo
Demonstrate aggregation operations on invoice data using
Object Expression, String Expression, and SparkSQL.

Run:   python -m src.jobs.agg_demo
Input: data/input/invoices.csv
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils import get_spark, get_logger

log = get_logger(__name__)

INPUT_PATH = "data/input/invoices.csv"


def run() -> None:
    spark = get_spark("AggDemo")
    log.header("Job: Aggregation Demo")

    invoice_df = (
        spark.read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(INPUT_PATH)
    )
    invoice_df.show()

    # ── Object Expression — count, sum, avg, countDistinct
    log.header("Object Expression")
    invoice_df.select(
        F.count("*").alias("Count *"),
        F.sum("Quantity").alias("TotalQuantity"),
        F.avg("UnitPrice").alias("AvgPrice"),
        F.countDistinct("InvoiceNo").alias("CountDistinct"), # type: ignore
    ).show()

    # ── String Expression — selectExpr
    log.header("String Expression")
    invoice_df.selectExpr(
        "count(1) as Count",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice",
    ).show()

    # ── SparkSQL — groupBy Country and InvoiceNo
    log.header("SparkSQL — Group by Country and InvoiceNo")
    invoice_df.createOrReplaceTempView("invoices")
    query = """
        SELECT   Country,
                 InvoiceNo,
                 sum(Quantity)                        AS TotalQuantity,
                 round(sum(Quantity * UnitPrice), 2)  AS InvoiceValue
        FROM     invoices
        GROUP BY Country, InvoiceNo
    """
    summary_sql: DataFrame = spark.sql(query)  # type: ignore[no-untyped-call]
    summary_sql.show()

    # ── Object Expression — same groupBy
    log.header("Object Expression — Group by Country and InvoiceNo")
    summary_df = (
        invoice_df
        .groupBy("Country", "InvoiceNo")
        .agg(
            F.sum("Quantity").alias("TotalQuantity"),
            F.round(F.sum(F.col("Quantity") * F.col("UnitPrice")), 2).alias("InvoiceValue"),
        )
    )
    summary_df.show()

    log.ok("Aggregation Demo done!")
    spark.stop()


if __name__ == "__main__":
    run()