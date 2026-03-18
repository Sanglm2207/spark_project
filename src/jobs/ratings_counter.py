"""
Job: Ratings Histogram
Count how many times each rating (1-5) appears across all MovieLens reviews.

Run:   python -m src.jobs.ratings_counter
Input: data/input/ml-100k/u.data  (tab-separated: user_id, movie_id, rating, timestamp)
"""

from __future__ import annotations

import collections

from src.utils import get_spark, get_logger

log = get_logger(__name__)


def run() -> None:
    spark = get_spark("RatingsHistogram")

    # SparkContext is the entry point for RDD operations —
    # retrieved from the existing session instead of creating a new one
    sc = spark.sparkContext

    # Each line: "user_id\tmovie_id\trating\ttimestamp"
    # split()[2] extracts the rating field (index 2)
    lines = sc.textFile("data/input/ml-100k/u.data")
    ratings = lines.map(lambda x: x.split()[2])

    # countByValue triggers a Spark job and returns a plain Python dict —
    # suitable here because the result has only 5 keys (ratings 1-5)
    result = ratings.countByValue()

    # Sort by rating value (1 → 5) for readable output
    sorted_results = collections.OrderedDict(sorted(result.items()))
    for key, value in sorted_results.items():
        log.info(f"Rating {key}: {value}")

    spark.stop()

if __name__ == "__main__":
    run()