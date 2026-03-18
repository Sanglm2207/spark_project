"""
Job: Average Friends by Age
Calculate the average number of friends for each age group using RDD API.

Run:   python -m src.jobs.avg_friends_by_age
Input: data/input/fakefriends.csv (id, name, age, num_friends)
"""

from __future__ import annotations

from src.utils import get_spark, get_logger

log = get_logger(__name__)

BAR_CHAR    = "█"
BAR_MAX_LEN = 30


def parse_line(line: str) -> tuple[int, int]:
    """
    Parse a CSV line into (age, num_friends).

    Input format: id,name,age,num_friends
    e.g. "0,Will,33,385" → (33, 385)
    """
    fields      = line.split(",")
    age         = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


def run() -> None:
    spark = get_spark("AvgFriendsByAge")
    sc    = spark.sparkContext
    log.header("Job: Average Friends by Age")

    lines = sc.textFile("data/input/fakefriends.csv")

    # Parse each line into (age, num_friends)
    rdd = lines.map(parse_line)

    def wrap_count(x: int) -> tuple[int, int]:
        return (x, int(1))

    def sum_counts(a: tuple[int, int], b: tuple[int, int]) -> tuple[int, int]:
        return (a[0] + b[0], a[1] + b[1])

    # mapValues: wrap num_friends into (num_friends, 1) to track sum and count
    # reduceByKey: sum both across same age → (total_friends, total_people)
    totals_by_age = (
        rdd
        .mapValues(wrap_count)
        .reduceByKey(sum_counts)
    )

    # Compute average: total_friends / total_people
    averages_by_age = totals_by_age.mapValues(lambda x: round(x[0] / x[1], 2))

    # Collect and sort by age for display
    results = sorted(averages_by_age.collect(), key=lambda x: x[0])

    print()
    print(f"  {'Age':>4}  {'Avg Friends':>11}")
    print(f"  {'─'*4}  {'─'*11}")
    for age, avg in results:
        print(f"  {age:>4}  {avg:>11.2f}")
    print()

    log.ok("Done!")
    spark.stop()


if __name__ == "__main__":
    run()