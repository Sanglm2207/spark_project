"""
Job: Word Count
Classic Spark example — count word frequency across lines of text.
Uses regex splitting to handle punctuation and multiple spaces.

Run:             python -m src.jobs.word_count
Run with file:   python -m src.jobs.word_count data/input/text.txt
"""

from __future__ import annotations

import sys

from pyspark.sql import Row
from pyspark.sql import functions as F
from src.utils import get_spark, get_logger

log = get_logger(__name__)

# Matches any sequence of non-word characters (spaces, punctuation, tabs) —
# more robust than splitting on a single space
WORD_SPLIT_REGEX = "\\W+"


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
        df = spark.createDataFrame(data)  # type: ignore[arg-type]
    else:
        df = spark.read.text(input_path).withColumnRenamed("value", "line")

    result = (
        df
        # Regex split handles punctuation, tabs, and multiple consecutive spaces —
        # e.g. "hello,world" → ["hello", "world"], not ["hello,world"]
        .select(F.explode(F.split(F.col("line"), WORD_SPLIT_REGEX)).alias("word"))
        # Normalize to lowercase so "Spark" and "spark" count as the same word
        .select(F.lower(F.col("word")).alias("word"))
        # Drop empty strings produced by leading/trailing delimiters
        .filter(F.col("word") != "")
        .groupBy("word")
        .agg(F.count("*").alias("count"))
        # Primary sort: frequency descending, secondary: alphabetical for tie-breaking
        .orderBy(F.col("count").desc(), F.col("word").asc())
    )

    log.info("Top words:")
    result.show(10, truncate=False)
    log.ok("Word Count done!")
    spark.stop()


if __name__ == "__main__":
    # Accept an optional file path: python -m src.jobs.word_count path/to/file.txt
    path: str | None = sys.argv[1] if len(sys.argv) > 1 else None
    run(path)