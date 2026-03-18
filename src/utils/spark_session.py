"""
SparkSession factory — single initialization point for the entire project.

All jobs call get_spark() instead of creating their own SparkSession,
keeping config consistent and easy to change in one place.
"""

from __future__ import annotations

from typing import Literal

from pyspark.sql import SparkSession

Env = Literal["local", "cluster"]


def get_spark(app_name: str = "SparkApp", env: Env = "local") -> SparkSession:
    """
    Return a configured SparkSession for the given environment.

    getOrCreate() ensures only one SparkSession exists per process —
    calling this again in the same process returns the existing session.

    Args:
        app_name: Name shown on Spark UI (localhost:4040)
        env: "local" for development | "cluster" for production

    Returns:
        SparkSession with logLevel set to ERROR to suppress INFO/WARN spam
    """
    builder: SparkSession.Builder = SparkSession.builder.appName(app_name)

    if env == "local":
        builder = (
            builder
            .master("local[*]")
            .config("spark.driver.memory", "1g")
            # Default 200 partitions causes heavy overhead when running locally —
            # 4 is sufficient for small datasets in dev/learning environments
            .config("spark.sql.shuffle.partitions", "4")
            # Disable progress bar — it spams the terminal when running multiple jobs
            .config("spark.ui.showConsoleProgress", "false")
        )

    spark: SparkSession = builder.getOrCreate()

    # Keep only ERROR to stay output clean —
    # temporarily switch to "INFO" when debugging Spark internals
    spark.sparkContext.setLogLevel("ERROR")
    return spark
