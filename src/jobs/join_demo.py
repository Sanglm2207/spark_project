"""
Job: Spark Join Demo
Join orders and products DataFrames to enrich order data with product info.

Run:   python -m src.jobs.join_demo
Input: inline sample data — no file needed
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Row
from src.utils import get_spark, get_logger

log = get_logger(__name__)


def run() -> None:
    spark = get_spark("JoinDemo")
    log.header("Job: Spark Join Demo")

    # ── Sample data ──────────────────────────────────────
    orders_list = [
        Row(order_id="01", prod_id="02", unit_price=350, qty=1),
        Row(order_id="01", prod_id="04", unit_price=580, qty=1),
        Row(order_id="01", prod_id="07", unit_price=320, qty=2),
        Row(order_id="02", prod_id="03", unit_price=450, qty=1),
        Row(order_id="02", prod_id="06", unit_price=220, qty=1),
        Row(order_id="03", prod_id="01", unit_price=195, qty=1),
        Row(order_id="04", prod_id="09", unit_price=270, qty=3),
        Row(order_id="04", prod_id="08", unit_price=410, qty=2),
        Row(order_id="05", prod_id="02", unit_price=350, qty=1),
    ]

    product_list = [
        Row(prod_id="01", prod_name="Scroll Mouse",        list_price=250, qty=20),
        Row(prod_id="02", prod_name="Optical Mouse",       list_price=350, qty=20),
        Row(prod_id="03", prod_name="Wireless Mouse",      list_price=450, qty=50),
        Row(prod_id="04", prod_name="Wireless Keyboard",   list_price=580, qty=50),
        Row(prod_id="05", prod_name="Standard Keyboard",   list_price=360, qty=10),
        Row(prod_id="06", prod_name="16 GB Flash Storage", list_price=240, qty=100),
        Row(prod_id="07", prod_name="32 GB Flash Storage", list_price=320, qty=50),
        Row(prod_id="08", prod_name="64 GB Flash Storage", list_price=430, qty=25),
    ]

    order_df:   DataFrame = spark.createDataFrame(orders_list)   # type: ignore[arg-type]
    product_df: DataFrame = spark.createDataFrame(product_list)  # type: ignore[arg-type]

    log.info("Orders:")
    order_df.show()

    log.info("Products:")
    product_df.show()

    # ── Inner Join ───────────────────────────────────────
    log.header("Inner Join — orders with product info")

    # Rename qty in product_df to avoid ambiguity after join
    product_renamed_df = product_df.withColumnRenamed("qty", "reorder_qty")

    # Join condition: order prod_id matches product prod_id
    join_expr = order_df["prod_id"] == product_renamed_df["prod_id"]

    (
        order_df
        .join(product_renamed_df, join_expr, "inner")
        # Drop duplicate prod_id from product side
        .drop(product_renamed_df["prod_id"])
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty")
        .show()
    )

    # ── Left Outer Join — keep all orders including unmatched ──
    log.header("Left Outer Join — all orders, null for unmatched products")
    (
        order_df
        .join(product_renamed_df, join_expr, "left")
        .drop(product_renamed_df["prod_id"])
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty")
        .show()
    )

    log.ok("Join Demo done!")
    spark.stop()


if __name__ == "__main__":
    run()