"""
Job: Most Popular Superhero
Find the Marvel superhero with the most connections,
and calculate the average number of connections per hero.

Run:   python -m src.jobs.most_popular_superhero
Input: data/input/Marvel_Names.csv
       data/input/Marvel_Graph.txt
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.utils import get_spark, get_logger

log = get_logger(__name__)

NAMES_PATH = "data/input/Marvel_Names.csv"
GRAPH_PATH = "data/input/Marvel_Graph.txt"

# Schema for Marvel_Names.csv — id and hero name
NAMES_SCHEMA = StructType([
    StructField("id",   IntegerType(), True),
    StructField("name", StringType(),  True),
])


def run() -> None:
    spark = get_spark("MostPopularSuperhero")
    log.header("Job: Most Popular Superhero")

    # ── Read data ────────────────────────────────────────
    names = spark.read.schema(NAMES_SCHEMA).csv(NAMES_PATH)
    names.show()

    # Each line: "hero_id conn1 conn2 conn3 ..."
    lines = spark.read.text(GRAPH_PATH)
    lines.show()

    # ── Count connections per hero ────────────────────────
    # split()[0]     → hero id (first token)
    # size(split())-1 → number of connections (remaining tokens)
    # A hero can appear on multiple lines — sum all counts per id
    connections: DataFrame = (
        lines
        .withColumn("id",          F.split(F.col("value"), " ")[0].cast(IntegerType()))
        .withColumn("connections", F.size(F.split(F.col("value"), " ")) - 1)
        .groupBy("id")
        .agg(F.sum("connections").alias("connections"))
    )
    connections.show()

    # ── Average connections ───────────────────────────────
    avg_df: DataFrame = connections.agg(
        F.avg("connections").alias("AVG_CONNECTIONS")
    )
    avg_df.show()

    # ── Join with names to get hero names ─────────────────
    join_expr = connections["id"] == names["id"]
    joined_df: DataFrame = (
        connections
        .join(names, join_expr)
        .drop(names["id"])
    )
    joined_df.show()

    # ── Find most popular hero ────────────────────────────
    # collect()[0] — safe here because result is a single row (max)
    most_popular = (
        joined_df
        .orderBy(F.col("connections").desc())
        .first()
    )

    if most_popular:
        print(
            f"\n  {most_popular['name']} is the most popular superhero"
            f" with {most_popular['connections']} co-appearances.\n"
        )

    log.ok("Most Popular Superhero done!")
    spark.stop()


if __name__ == "__main__":
    run()