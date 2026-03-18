"""
Job: Column Demo
Demonstrate Column String, Column Object, and Expression approaches
to select and transform columns in a DataFrame.

Run:   python -m src.jobs.column_demo
Input: data/input/fly.csv
"""

from __future__ import annotations

from pyspark.sql import functions as F
from src.utils import get_spark, get_logger

log = get_logger(__name__)

INPUT_PATH = "data/input/fly.csv"


def run() -> None:
    spark = get_spark("ColumnDemo")
    log.header("Job: Column Demo")

    df = (
        spark.read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .option("samplingRatio", "0.0001")
             .load(INPUT_PATH)
    )

    df.printSchema()
    df.show(5)

    # ── Column String — pass column names as plain strings
    log.header("Column String")
    df.select("Origin", "Dest", "Distance").show(10)

    # ── Column Object — use col() for type-safe column references
    log.header("Column Object")
    df.select(F.col("Origin"), F.col("Dest"), F.col("Distance")).show(10)

    # ── String Expression (selectExpr) — SQL syntax inline
    log.header("String Expression — FlightDate")
    df.selectExpr(
        "Origin", "Dest", "Distance",
        "to_date(concat(Year, Month, DayofMonth), 'yyyyMMdd') as FlightDate"
    ).show(10)

    # ── Column Object Expression — equivalent using col() and F.to_date
    log.header("Column Object Expression — FlightDate")
    df.select(
        "Origin", "Dest", "Distance",
        F.to_date(
            F.concat(F.col("Year"), F.col("Month"), F.col("DayofMonth")),
            "yyyyMMdd"
        ).alias("FlightDate")
    ).show(10)

    log.ok("Column Demo done!")
    spark.stop()


if __name__ == "__main__":
    run()