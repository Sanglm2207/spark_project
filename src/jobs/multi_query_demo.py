"""
Job: Multi Query Demo
Read invoice stream from Kafka, process into 2 parallel streams:
  1. Notification sink  → Kafka topic "notifications"
  2. Flattened invoice  → File sink (JSON)

Run:
    # 1. Start Kafka
    docker-compose up -d

    # 2. Send sample data
    python src/utils/kafka_producer.py

    # 3. Run this job
    python -m src.jobs.multi_query_demo

Input:  Kafka topic "invoices"
Output: Kafka topic "notifications" + data/output/invoices_flat/
"""

from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from src.utils import get_spark, get_logger

log = get_logger(__name__)

BOOTSTRAP_SERVERS  = "localhost:9092"
INPUT_TOPIC        = "invoices"
NOTIFY_TOPIC       = "notifications"
OUTPUT_DIR         = "data/output/invoices_flat"
CHECKPOINT_NOTIFY  = "data/output/chk-notify"
CHECKPOINT_INVOICE = "data/output/chk-invoice"

# Full invoice schema — must match JSON structure from Kafka
INVOICE_SCHEMA = StructType([
    StructField("InvoiceNumber",   StringType()),
    StructField("CreatedTime",     LongType()),
    StructField("StoreID",         StringType()),
    StructField("PosID",           StringType()),
    StructField("CashierID",       StringType()),
    StructField("CustomerType",    StringType()),
    StructField("CustomerCardNo",  StringType()),
    StructField("TotalAmount",     DoubleType()),
    StructField("NumberOfItems",   IntegerType()),
    StructField("PaymentMethod",   StringType()),
    StructField("CGST",            DoubleType()),
    StructField("SGST",            DoubleType()),
    StructField("CESS",            DoubleType()),
    StructField("DeliveryType",    StringType()),
    StructField("DeliveryAddress", StructType([
        StructField("AddressLine",    StringType()),
        StructField("City",           StringType()),
        StructField("State",          StringType()),
        StructField("PinCode",        StringType()),
        StructField("ContactNumber",  StringType()),
    ])),
    StructField("InvoiceLineItems", ArrayType(StructType([
        StructField("ItemCode",        StringType()),
        StructField("ItemDescription", StringType()),
        StructField("ItemPrice",       DoubleType()),
        StructField("ItemQty",         IntegerType()),
        StructField("TotalValue",      DoubleType()),
    ]))),
])


def run() -> None:
    # spark-sql-kafka package must be specified at session creation —
    # cannot be added after SparkSession already exists
    # Version must match PySpark exactly — PySpark 4.1.1 uses Spark 4.1.1
    import os
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 pyspark-shell"
    )

    spark = get_spark("MultiQueryDemo")
    spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    log.header("Job: Multi Query Demo")

    # ── Read from Kafka ──────────────────────────────────
    kafka_df = (
        spark.readStream
             .format("kafka")
             .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
             .option("subscribe", INPUT_TOPIC)
             # earliest — read all messages from beginning of topic
             .option("startingOffsets", "earliest")
             .load()
    )

    # Kafka value is binary — cast to string then parse as JSON
    value_df = kafka_df.select(
        F.from_json(F.col("value").cast("string"), INVOICE_SCHEMA).alias("value")
    )

    # ── Stream 1: Notification → Kafka Sink ─────────────
    notification_df = (
        value_df
        .select("value.InvoiceNumber", "value.CustomerCardNo", "value.TotalAmount")
        .withColumn("EarnedLoyaltyPoints", F.expr("TotalAmount * 0.2"))
    )

    # Kafka sink requires key-value format — serialize notification as JSON string
    kafka_target_df = notification_df.selectExpr(
        "InvoiceNumber as key",
        "to_json(struct(*)) as value",
    )

    notification_writer_query = (
        kafka_target_df.writeStream
        .queryName("Notification Writer")
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", NOTIFY_TOPIC)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_NOTIFY)
        .start()
    )

    # ── Stream 2: Flattened Invoice → File Sink ──────────
    explode_df = value_df.selectExpr(
        "value.InvoiceNumber", "value.CreatedTime", "value.StoreID",
        "value.PosID", "value.CustomerType", "value.PaymentMethod",
        "value.DeliveryType",
        "value.DeliveryAddress.City",
        "value.DeliveryAddress.State",
        "value.DeliveryAddress.PinCode",
        "explode(value.InvoiceLineItems) as LineItem",
    )

    flattened_df = (
        explode_df
        .withColumn("ItemCode",        F.expr("LineItem.ItemCode"))
        .withColumn("ItemDescription", F.expr("LineItem.ItemDescription"))
        .withColumn("ItemPrice",       F.expr("LineItem.ItemPrice"))
        .withColumn("ItemQty",         F.expr("LineItem.ItemQty"))
        .withColumn("TotalValue",      F.expr("LineItem.TotalValue"))
        .drop("LineItem")
    )

    invoice_writer_query = (
        flattened_df.writeStream
        .format("json")
        .queryName("Flattened Invoice Writer")
        .outputMode("append")
        .option("path", OUTPUT_DIR)
        .option("checkpointLocation", CHECKPOINT_INVOICE)
        .start()
    )

    log.ok("Both queries started — waiting for termination")
    log.info("Press Ctrl+C to stop")

    try:
        # awaitAnyTermination — block until either query stops
        spark.streams.awaitAnyTermination()
    except (KeyboardInterrupt, Exception):
        log.warn("Stopping all streams...")
        # stop() with awaitTermination=False — don't wait for JVM response
        # avoids Py4JNetworkError when JVM is already shutting down
        try:
            notification_writer_query.stop()
        except Exception:
            pass
        try:
            invoice_writer_query.stop()
        except Exception:
            pass
        log.ok("All streams stopped.")
    finally:
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    run()