"""
Job: Tumbling Window Demo
Calculate total Buy and Sell values per 15-minute tumbling window
from a Kafka stream of stock trades.

Run:
    docker-compose up -d
    python src/utils/kafka_producer.py  (with trades data)
    python -m src.jobs.tumbling_window_demo

Input:  Kafka topic "trades"
Output: console (update mode)
"""

from __future__ import annotations

import os

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from src.utils import get_spark, get_logger

log = get_logger(__name__)

BOOTSTRAP_SERVERS  = "localhost:9092"
INPUT_TOPIC        = "trades"
CHECKPOINT_DIR     = "data/output/chk-tumbling-window"

# spark-sql-kafka version must match PySpark exactly
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 pyspark-shell"
)

# Stock trade schema — each message contains trade type (Buy/Sell), amount, and time
STOCK_SCHEMA = StructType([
    StructField("CreatedTime", StringType(),  True),
    StructField("Type",        StringType(),  True),
    StructField("Amount",      IntegerType(), True),
    StructField("BrokerCode",  StringType(),  True),
])


def run() -> None:
    spark = get_spark("TumblingWindowDemo")
    spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    log.header("Job: Tumbling Window Demo")

    # ── Read from Kafka ──────────────────────────────────
    kafka_df = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
             .option("subscribe", INPUT_TOPIC)
             .option("startingOffsets", "earliest")
             .load()
    )

    # Kafka value is binary — cast to string then parse as JSON
    value_df = kafka_df.select(
        F.from_json(F.col("value").cast("string"), STOCK_SCHEMA).alias("value")
    )

    # Extract fields — convert CreatedTime string to timestamp for windowing
    # Buy/Sell are derived from Amount based on Type column
    trade_df = (
        value_df
        .select("value.*")
        .withColumn("CreatedTime", F.to_timestamp(F.col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))
        # Buy amount: Amount when Type is BUY, else 0
        .withColumn("Buy",  F.expr("CASE WHEN Type == 'BUY'  THEN Amount ELSE 0 END"))
        # Sell amount: Amount when Type is SELL, else 0
        .withColumn("Sell", F.expr("CASE WHEN Type == 'SELL' THEN Amount ELSE 0 END"))
    )

    # ── Tumbling Window — 15 minute buckets ─────────────
    # window(timeColumn, windowDuration) — non-overlapping fixed intervals
    # Each trade falls into exactly one 15-minute window
    window_agg_df = (
        trade_df
        .groupBy(F.window(F.col("CreatedTime"), "15 minute"))
        .agg(
            F.sum("Buy").alias("TotalBuy"),
            F.sum("Sell").alias("TotalSell"),
        )
    )

    # Extract window start/end for readable output
    output_df = window_agg_df.select(
        F.col("window.start").alias("start"),
        F.col("window.end").alias("end"),
        F.col("TotalBuy"),
        F.col("TotalSell"),
    )

    # ── Write to console ─────────────────────────────────
    # outputMode update — only show windows that changed in this batch
    window_query = (
        output_df.writeStream
        .format("console")
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="1 minute")
        .start()
    )

    log.ok("Tumbling Window query started")
    log.info("Press Ctrl+C to stop")

    try:
        window_query.awaitTermination()
    except (KeyboardInterrupt, Exception):
        log.warn("Stopping stream...")
        try:
            window_query.stop()
        except Exception:
            pass
        log.ok("Stream stopped.")
    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    run()