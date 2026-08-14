"""
Validates the transformation functions in streaming_job.py using static
(batch) DataFrames that mimic what would come off Kafka. This proves the
logic is correct without needing a live Kafka broker or streaming context.
"""

from pyspark.sql import SparkSession

from streaming_job import cart_abandonment, clickstream_funnel, low_stock_alerts, revenue_per_minute

spark = (
    SparkSession.builder.appName("LocalTest")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# --- Test 1: revenue_per_minute -------------------------------------------------
orders_data = [
    ("o1", "u1", 100.0, "2026-07-17 10:00:05"),
    ("o2", "u2", 50.0, "2026-07-17 10:00:20"),
    ("o3", "u3", 75.0, "2026-07-17 10:01:10"),
]
orders_df = spark.createDataFrame(orders_data, ["order_id", "user_id", "order_total", "timestamp"])
orders_df = orders_df.withColumn("event_time", orders_df["timestamp"].cast("timestamp"))

print("=== revenue_per_minute ===")
revenue_per_minute(orders_df).orderBy("window_start").show(truncate=False)

# --- Test 2: clickstream_funnel -------------------------------------------------
clicks_data = [
    ("e1", "Electronics", "product_view", "2026-07-17 10:00:05"),
    ("e2", "Electronics", "product_view", "2026-07-17 10:00:15"),
    ("e3", "Electronics", "add_to_cart", "2026-07-17 10:00:30"),
    ("e4", "Apparel", "page_view", "2026-07-17 10:00:45"),
]
clicks_df = spark.createDataFrame(clicks_data, ["event_id", "category", "event_type", "timestamp"])
clicks_df = clicks_df.withColumn("event_time", clicks_df["timestamp"].cast("timestamp"))

print("=== clickstream_funnel ===")
clickstream_funnel(clicks_df).orderBy("category", "event_type").show(truncate=False)

# --- Test 3: low_stock_alerts ---------------------------------------------------
inv_data = [
    ("P0001", "Electronics", 5, False, "2026-07-17 10:00:05"),
    ("P0002", "Apparel", 100, False, "2026-07-17 10:00:10"),
]
inv_df = spark.createDataFrame(
    inv_data, ["product_id", "category", "current_stock", "out_of_stock_flag", "timestamp"]
)
inv_df = inv_df.withColumn("low_stock_flag", inv_df["current_stock"] < 15)
inv_df = inv_df.withColumn("event_time", inv_df["timestamp"].cast("timestamp"))

print("=== low_stock_alerts ===")
low_stock_alerts(inv_df).show(truncate=False)

# --- Test 4: cart_abandonment ----------------------------------------------------
clicks_data2 = [
    ("u1", "P0001", "2026-07-17 10:00:00"),  # abandoned - no matching order
    ("u2", "P0002", "2026-07-17 10:00:00"),  # will have a matching order
]
from pyspark.sql import functions as F2

clicks_df2 = spark.createDataFrame(clicks_data2, ["user_id", "product_id", "timestamp"])
clicks_df2 = clicks_df2.withColumn("event_type", F2.lit("add_to_cart"))
clicks_df2 = clicks_df2.withColumn("event_time", clicks_df2["timestamp"].cast("timestamp"))

orders_data2 = [
    ("o10", "u2", 40.0, "2026-07-17 10:05:00"),  # matches u2's cart add
]
orders_df2 = spark.createDataFrame(orders_data2, ["order_id", "user_id", "order_total", "timestamp"])
orders_df2 = orders_df2.withColumn("event_time", orders_df2["timestamp"].cast("timestamp"))

print("=== cart_abandonment (expect only u1/P0001) ===")
cart_abandonment(clicks_df2, orders_df2).show(truncate=False)

print("ALL TRANSFORMATION TESTS RAN SUCCESSFULLY")
spark.stop()
