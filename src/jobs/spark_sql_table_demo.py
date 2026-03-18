"""
Job: SparkSQL Table Demo
Create a Managed Table in Spark catalog using flight data.

Run:   python -m src.jobs.spark_sql_table_demo
Input: data/input/flight-time.parquet  (parquet version of flight-time.csv)

Managed Table — Spark controls both metadata and data.
Data is stored under spark.sql.warehouse.dir.
Dropping the table removes both metadata and data.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from src.utils import get_spark, get_logger

log = get_logger(__name__)

INPUT_PATH = "data/input/flight-time.parquet"
DATABASE   = "AIRLINE_DB"
TABLE      = "flight_data_tbl"


def run() -> None:
    spark = get_spark("SparkSQLTableDemo")
    log.header("Job: SparkSQL Table Demo")

    # Read flight data from parquet
    df = spark.read.format("parquet").load(INPUT_PATH)
    log.info(f"Loaded {df.count()} rows from {INPUT_PATH}")
    df.printSchema()

    # Create database if not exists — equivalent to a namespace/schema in SQL
    create_db = f"CREATE DATABASE IF NOT EXISTS {DATABASE}"
    spark.sql(create_db)  # type: ignore[no-untyped-call]
    spark.catalog.setCurrentDatabase(DATABASE)
    log.info(f"Using database: {DATABASE}")

    # Write as Managed Table — Spark stores data in warehouse dir
    df.write \
      .mode("overwrite") \
      .saveAsTable(TABLE)
    log.ok(f"Managed table '{TABLE}' created in '{DATABASE}'")

    # List tables in database to verify
    tables = spark.catalog.listTables(DATABASE)
    log.info("Tables in database:")
    for t in tables:
        print(f"  {t.name:<30} type={t.tableType}  temp={t.isTemporary}")

    # Query via SparkSQL to verify data is accessible
    log.header("Query via SparkSQL")
    flight_query = f"""
        SELECT   OP_CARRIER,
                 COUNT(*)        AS flights,
                 AVG(DISTANCE)   AS avg_distance
        FROM     {TABLE}
        GROUP BY OP_CARRIER
        ORDER BY flights DESC
        LIMIT    10
    """
    result: DataFrame = spark.sql(flight_query)  # type: ignore[no-untyped-call]
    result.show()

    log.ok("SparkSQL Table Demo done!")
    spark.stop()


if __name__ == "__main__":
    run()