"""
Shared DataFrame helpers for the entire project.

All I/O (read/write) goes through here for centralized control —
adding logging, metrics, or retry logic only requires changes in one place.
"""

from __future__ import annotations

from typing import Literal

from pyspark.sql import SparkSession, DataFrame
from src.utils.logger import get_logger

log = get_logger(__name__)

WriteMode = Literal["overwrite", "append", "ignore", "error"]


def read_csv(spark: SparkSession, path: str, header: bool = True) -> DataFrame:
    """
    Read a CSV file with Spark schema inference.

    Avoid this for files > 1GB — schema inference forces Spark to
    scan the entire file twice. Use read_parquet or pass an explicit schema instead.
    """
    log.info(f"Reading CSV: {path}")
    return spark.read.option("header", str(header).lower()).csv(path)


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """
    Read Parquet — prefer this over CSV for large data.

    Parquet embeds the schema in the file header, so no inference is needed.
    Also supports column pruning (only reads the columns you select).
    """
    log.info(f"Reading Parquet: {path}")
    return spark.read.parquet(path)


def save_parquet(df: DataFrame, path: str, mode: WriteMode = "overwrite") -> None:
    """
    Write a DataFrame to Parquet.

    Args:
        mode: "overwrite" replaces existing data | "append" keeps it —
              use "append" carefully, it can produce duplicates if a job reruns
    """
    log.info(f"Saving Parquet → {path}  (mode={mode})")
    df.write.mode(mode).parquet(path)
    log.ok("Saved!")


def show_summary(df: DataFrame, label: str = "DataFrame") -> None:
    """
    Print schema + 5 sample rows + total row count.

    count() triggers a Spark job — use only during debugging,
    not in production pipelines where it adds unnecessary overhead.
    """
    log.header(label)
    df.printSchema()
    df.show(5, truncate=False)
    log.info(f"Total rows: {df.count()}")
