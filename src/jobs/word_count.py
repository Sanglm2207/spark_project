"""
Job: Word Count
Classic Spark example — count word frequency across lines of text.

Run:             python -m src.jobs.word_count
Run with file:   python -m src.jobs.word_count data/input/text.txt
"""

from __future__ import annotations

import sys

from pyspark.sql import Row
from pyspark.sql import functions as F
from src.utils import get_spark, get_logger

log = get_logger(__name__)


def run(input_path: str | None = None) -> None:
    spark = get_spark("WordCount")
    log.header("Job: Word Count")

    if input_path is None:
        log.warn("No input_path provided — using sample data")
        data: list[Row] = [
            Row(line="Apache Spark is a framework for large-scale data processing"),
            Row(line="Spark supports Python Java Scala and R"),
            Row(line="PySpark is the Python API for Apache Spark"),
        ]
        # pyspark-stubs does not yet support PySpark 4.x —
        # createDataFrame overloads are unresolvable until stubs are updated
        df = spark.createDataFrame(data) # type: ignore[arg-type]
    else:
        df = spark.read.text(input_path).withColumnRenamed("value", "line")

    result = (
        df
        # Split each line into individual words
        .select(F.explode(F.split(F.col("line"), " ")).alias("word"))
        # Drop empty strings produced by consecutive spaces
        .filter(F.col("word") != "")
        .groupBy("word")
        .agg(F.count("*").alias("count"))
        .orderBy(F.col("count").desc())
    )

    log.info("Top words:")
    result.show(10, truncate=False)
    log.ok("Word Count done!")
    spark.stop()


if __name__ == "__main__":
    # Accept an optional file path: python -m src.jobs.word_count path/to/file.txt
    path: str | None = sys.argv[1] if len(sys.argv) > 1 else None
    run(path)
