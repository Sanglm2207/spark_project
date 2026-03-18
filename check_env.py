"""
Environment check — run this before executing any job.
Run: python check_env.py
"""

from __future__ import annotations

import os
import sys
import time

from src.utils import get_spark, get_logger

log = get_logger("check_env")


def check_python() -> None:
    log.header("1 / 3  Python")
    py = sys.version_info
    if py.major == 3 and 10 <= py.minor <= 12:
        log.ok(f"Python {py.major}.{py.minor}.{py.micro}")
    else:
        log.warn(f"Python {py.major}.{py.minor}.{py.micro}  (recommended: 3.10–3.12)")


def check_java() -> None:
    log.header("2 / 3  Java")
    java_home = os.environ.get("JAVA_HOME", "")
    if java_home:
        log.ok(f"JAVA_HOME = {java_home}")
    else:
        log.warn("JAVA_HOME is not set")

    java_ver = os.popen("java -version 2>&1").read().strip().split("\n")[0]
    if java_ver:
        log.ok(f"java  →  {java_ver}")
    else:
        log.error("java not found in PATH")


def check_spark() -> None:
    log.header("3 / 3  SparkSession")
    t0 = time.time()
    spark = get_spark("EnvCheck")
    log.ok(f"SparkSession ready  ({time.time() - t0:.1f}s)")
    log.ok(f"Spark {spark.version}  |  cores = {spark.sparkContext.defaultParallelism}")
    spark.stop()


if __name__ == "__main__":
    check_python()
    check_java()
    check_spark()
    print()
    log.ok("Environment is ready!\n")
