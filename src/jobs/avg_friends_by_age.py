"""
Job: Average Friends by Age
Calculate the average number of friends for each age group.

Run:   python -m src.jobs.avg_friends_by_age
Input: data/input/fakefriends.csv (id, name, age, num_friends)
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.utils import get_spark, get_logger

log = get_logger(__name__)

# Explicit schema — no header in file so column names must be declared manually
SCHEMA = StructType([
    StructField("id",          IntegerType(), False),
    StructField("name",        StringType(),  True),
    StructField("age",         IntegerType(),  True),
    StructField("num_friends", IntegerType(),  True),
])


def run() -> None:
    spark = get_spark("AvgFriendsByAge")
    log.header("Job: Average Friends by Age")

    df = spark.read.schema(SCHEMA).csv("data/input/fakefriends.csv")

    result = (
        df.groupBy("age")
          .agg(F.round(F.avg("num_friends"), 2).alias("avg_friends"))
          .orderBy("age")
    )

    log.info("Average friends per age:")
    result.show(truncate=False)
    log.ok("Done!")
    spark.stop()


if __name__ == "__main__":
    run()