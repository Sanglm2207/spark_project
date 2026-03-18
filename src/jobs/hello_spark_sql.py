"""
Job: Hello SparkSQL
Count people under 40 by country using SparkSQL temp view.

Run:   python -m src.jobs.hello_spark_sql
Input: data/input/survey.csv
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from src.utils import get_spark, get_logger

log = get_logger(__name__)

INPUT_PATH = "data/input/survey.csv"


def run() -> None:
    spark = get_spark("HelloSparkSQL")
    log.header("Job: Hello SparkSQL")

    df = (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(INPUT_PATH)
    )

    log.info(f"Total rows: {df.count()}")
    df.printSchema()

    # Register as temp view — only lives for the duration of this SparkSession
    df.createOrReplaceTempView("survey")

    # SparkSQL — count people under 40 grouped by country
    query = """
        SELECT   Country,
                 COUNT(*) AS count
        FROM     survey
        WHERE    Age < 40
        GROUP BY Country
        ORDER BY count DESC
    """
    count_df: DataFrame = spark.sql(query)  # type: ignore[no-untyped-call]

    log.info("People under 40 by country:")
    count_df.show()
    log.ok("Hello SparkSQL done!")
    spark.stop()


if __name__ == "__main__":
    run()