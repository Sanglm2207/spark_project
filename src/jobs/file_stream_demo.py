"""
Job: File Stream Demo
Read JSON invoice files using Spark Structured Streaming,
flatten InvoiceLineItems array, and write to JSON output.

Run:   python -m src.jobs.file_stream_demo
Input: data/input/invoices_stream/  (directory with JSON files)
Output: data/output/invoices_flattened/ (streaming output)
"""

from __future__ import annotations

from pyspark.sql import functions as F
from src.utils import get_spark, get_logger

log = get_logger(__name__)

INPUT_DIR      = "data/input/invoices_stream"
OUTPUT_DIR     = "data/output/invoices_flattened"
CHECKPOINT_DIR = "data/output/chk-point-dir"


def run() -> None:
    # Structured Streaming needs more cores than default local[*] for concurrent read/write
    # stopGracefullyOnShutdown — wait for current batch to finish before stopping
    # schemaInference — infer schema from JSON files automatically
    spark = get_spark("FileStreamDemo")
    spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    spark.conf.set("spark.sql.streaming.schemaInference",      "true")

    log.header("Job: File Stream Demo")

    # ── Read streaming source ────────────────────────────
    # maxFilesPerTrigger — process at most 1 file per batch
    # to simulate real-time ingestion one file at a time
    raw_df = (
        spark.readStream
             .format("json")
             .option("path", INPUT_DIR)
             .option("maxFilesPerTrigger", 1)
             .load()
    )

    # ── Flatten InvoiceLineItems array ───────────────────
    # explode — turns each element of the array into a separate row
    explode_df = raw_df.selectExpr("explode(InvoiceLineItems) as LineItem")

    # Extract nested struct fields into top-level columns
    flattened_df = (
        explode_df
        .withColumn("ItemCode",        F.expr("LineItem.ItemCode"))
        .withColumn("ItemDescription", F.expr("LineItem.ItemDescription"))
        .withColumn("ItemPrice",       F.expr("LineItem.ItemPrice"))
        .withColumn("ItemQty",         F.expr("LineItem.ItemQty"))
        .withColumn("TotalValue",      F.expr("LineItem.TotalValue"))
        .drop("LineItem")
    )

    # ── Write streaming sink ─────────────────────────────
    # outputMode append — only write new rows each batch (no updates)
    # trigger processingTime — wait 1 minute between each batch
    # checkpointLocation — tracks progress so stream can resume after failure
    invoice_writer_query = (
        flattened_df.writeStream
        .format("json")
        .queryName("Flattened Invoice Writer")
        .outputMode("append")
        .option("path", OUTPUT_DIR)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="1 minute")
        .start()
    )

    log.ok("Flattened Invoice Writer started")

    # awaitTermination — block until stream is stopped manually or on error
    invoice_writer_query.awaitTermination()


if __name__ == "__main__":
    run()