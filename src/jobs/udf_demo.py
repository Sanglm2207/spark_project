"""
Job: UDF Demo
Normalize free-text Gender values using a regex-based UDF.
Demonstrates two registration methods: udf() and spark.udf.register().

Run:   python -m src.jobs.udf_demo
Input: data/input/survey_lab5.csv
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from src.utils import get_spark, get_logger

log = get_logger(__name__)

INPUT_PATH = "data/input/survey_lab5.csv"

FEMALE_PATTERN = r"^f$|f.m|w.m"
MALE_PATTERN   = r"^m$|ma|m.l"


def parse_gender(gender: str) -> str:
    """
    Normalize free-text gender to Female / Male / Unknown using regex.

    Patterns match common variations:
    - Female: "f", "female", "woman", "fem", etc.
    - Male:   "m", "male", "man", "mal", etc.
    Returns "Unknown" for anything unrecognized.
    """
    if not gender:
        return "Unknown"
    normalized = gender.strip().lower()
    if re.search(FEMALE_PATTERN, normalized):
        return "Female"
    if re.search(MALE_PATTERN, normalized):
        return "Male"
    return "Unknown"


def run() -> None:
    spark = get_spark("UDFDemo")
    log.header("Job: UDF Demo")

    # Read and display raw data
    survey_df = (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(INPUT_PATH)
    )
    log.info("Raw data:")
    survey_df.show(10)

    # ── Method 1: udf() — Object Expression
    log.header("Method 1: udf() — Object Expression")
    parse_gender_udf = F.udf(parse_gender, StringType())

    log.info("Catalog Entry:")
    for fn in spark.catalog.listFunctions():
        if "parse_gender" in fn.name:
            print(fn)

    # Apply UDF via Column Object — withColumn replaces Gender in-place
    survey_df1: DataFrame = survey_df.withColumn("Gender", parse_gender_udf(F.col("Gender")))
    survey_df1.show(10)

    # ── Method 2: spark.udf.register() — String Expression
    log.header("Method 2: spark.udf.register() — String Expression")
    spark.udf.register("parse_gender_udf", parse_gender, StringType())  # type: ignore[arg-type]

    log.info("Catalog Entry:")
    for fn in spark.catalog.listFunctions():
        if "parse_gender" in fn.name:
            print(fn)

    survey_df.createOrReplaceTempView("survey")
    # Apply UDF via selectExpr — SQL string expression
    query = "SELECT *, parse_gender_udf(Gender) AS Gender FROM survey"
    survey_df2: DataFrame = spark.sql(query)  # type: ignore[no-untyped-call]
    survey_df2.drop("Gender")  # drop old Gender, keep parsed one
    survey_df2.show(10)

    log.ok("UDF Demo done!")
    spark.stop()


if __name__ == "__main__":
    run()