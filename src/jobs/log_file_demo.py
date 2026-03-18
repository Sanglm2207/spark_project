"""
Job: Log File Demo
Extract structured fields from raw web server log lines using regex,
then count referrer domain occurrences.

Run:   python -m src.jobs.log_file_demo
Input: data/input/apache_logs.txt
Output: referrer domain | count
"""

from __future__ import annotations

from pyspark.sql import functions as F
from src.utils import get_spark, get_logger

log = get_logger(__name__)

INPUT_PATH = "data/input/apache_logs.txt"

# Apache Combined Log Format — each group captures one field:
# 1=ip, 4=date, 6=request, 10=referrer, 11=user_agent
LOG_REGEX = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'


def run() -> None:
    spark = get_spark("LogFileDemo")
    log.header("Job: Log File Demo")

    file_df = spark.read.text(INPUT_PATH)
    log.info(f"Total log lines: {file_df.count()}")

    # Extract fields from each log line using regex capture groups
    logs_df = file_df.select(
        F.regexp_extract(F.col("value"), LOG_REGEX, 1).alias("ip"),
        F.regexp_extract(F.col("value"), LOG_REGEX, 4).alias("date"),
        F.regexp_extract(F.col("value"), LOG_REGEX, 6).alias("request"),
        F.regexp_extract(F.col("value"), LOG_REGEX, 10).alias("referrer"),
    )

    log.info("Extracted fields:")
    logs_df.printSchema()
    logs_df.show(5, truncate=True)

    result = (
        logs_df
        # Keep only the domain: "http://www.google.com/path" → "http://www.google.com"
        .withColumn("referrer", F.substring_index(F.col("referrer"), "/", 3))
        # Drop rows with no referrer or placeholder "-"
        .where(F.col("referrer") != "-")
        .where(F.col("referrer") != "")
        .groupBy("referrer")
        .count()
        .orderBy(F.col("count").desc())
    )

    log.info("Referrer domain counts:")
    result.show(100, truncate=False)
    log.ok("Log File Demo done!")
    spark.stop()


if __name__ == "__main__":
    run()