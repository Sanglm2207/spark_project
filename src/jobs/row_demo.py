"""
Job: Row Demo
Convert string dates to date type using withColumn and to_date.

Run:   python -m src.jobs.row_demo
Input: no file — uses inline sample data
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from src.utils import get_spark, get_logger

log = get_logger(__name__)


def to_date_df(df: DataFrame, date_format: str, field: str) -> DataFrame:
    """
    Convert a string column to DateType using the given format.

    Args:
        df:          Input DataFrame
        date_format: Java date format string e.g. "M/d/yyyy"
        field:       Name of the column to convert
    """
    return df.withColumn(field, F.to_date(F.col(field), date_format))


def run() -> None:
    spark = get_spark("RowDemo")
    log.header("Job: Row Demo")

    my_rows = [
        Row("123", "04/05/2020"),
        Row("124", "4/5/2020"),
        Row("125", "04/5/2020"),
        Row("126", "4/05/2020"),
    ]

    my_schema = StructType([
        StructField("ID",        StringType(), True),
        StructField("EventDate", StringType(), True),
    ])

    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df  = spark.createDataFrame(my_rdd, my_schema)  # type: ignore[arg-type]

    log.info("Before conversion:")
    my_df.printSchema()
    my_df.show()

    # Convert EventDate from string to date — format handles variable-length months/days
    new_df = to_date_df(my_df, "M/d/yyyy", "EventDate")

    log.info("After conversion:")
    new_df.printSchema()
    new_df.show()

    log.ok("Row Demo done!")
    spark.stop()


if __name__ == "__main__":
    run()