"""
Job: Minimum Temperature by Station
Find the minimum temperature recorded at each weather station in 1800.

Run:   python -m src.jobs.min_temperature
Input: data/input/1800.csv
       (station_id, date, measure_type, temperature, ...)
       Temperature unit: tenths of a degree Celsius (e.g. -148 = -14.8°C)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.utils import get_spark, get_logger

log = get_logger(__name__)

# No header in file — columns declared explicitly
SCHEMA = StructType([
    StructField("station_id",   StringType(),  False),
    StructField("date",         StringType(),  True),
    StructField("measure_type", StringType(),  True),
    StructField("temperature",  IntegerType(), True),
])


def render_results(df: DataFrame) -> None:
    """
    Print a sorted table of minimum temperatures per station.
    Temperatures are converted from tenths of °C to °C for readability.
    """
    rows = df.collect()
    if not rows:
        log.warn("No data to display")
        return

    print()
    print(f"  {'Station':<20}  {'Min Temp (°C)':>13}")
    print(f"  {'─'*20}  {'─'*13}")

    for row in rows:
        station  = row["station_id"]
        temp_c   = row["min_temp_c"]
        # Visual indicator for extreme cold
        marker = " 🥶" if temp_c < -20 else ""
        print(f"  {station:<20}  {temp_c:>12.1f}°{marker}")

    print()


def run() -> None:
    spark = get_spark("MinTemperature")
    log.header("Job: Minimum Temperature by Station")

    df = spark.read.schema(SCHEMA).csv("data/input/1800.csv")

    result = (
        df
        # Only care about minimum temperature readings
        .filter(F.col("measure_type") == "TMIN")
        .groupBy("station_id")
        .agg(F.min("temperature").alias("min_temp_raw"))
        # Convert from tenths of °C to °C
        .withColumn("min_temp_c", F.round(F.col("min_temp_raw") / 10, 1))
        .drop("min_temp_raw")
        .orderBy("min_temp_c")
    )

    log.info(f"Stations found: {result.count()}")
    render_results(result)
    log.ok("Done!")
    spark.stop()


if __name__ == "__main__":
    run()