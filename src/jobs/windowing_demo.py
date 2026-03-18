"""
Job: Windowing Demo
Calculate running total of InvoiceValue per Country across weeks
using Spark Window functions.

Run:   python -m src.jobs.windowing_demo
Input: data/input/summary.parquet
"""

from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F
from src.utils import get_spark, get_logger

log = get_logger(__name__)

INPUT_PATH = "data/input/summary.parquet"


def run() -> None:
    spark = get_spark("WindowingDemo")
    log.header("Job: Windowing Demo")

    summary_df = spark.read.parquet(INPUT_PATH)
    summary_df.printSchema()

    # Window spec:
    # partitionBy — group rows by Country (each country gets its own running total)
    # orderBy     — sort by WeekNumber within each partition
    # rowsBetween — from the first row of the partition to the current row (cumulative sum)
    running_total_window = (
        Window
        .partitionBy("Country")
        .orderBy("WeekNumber")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    summary_df.withColumn(
        "RunningTotal",
        F.sum("InvoiceValue").over(running_total_window)
    ).show()

    log.ok("Windowing Demo done!")
    spark.stop()


if __name__ == "__main__":
    run()