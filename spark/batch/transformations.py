"""
PySpark Silver-to-Gold Data Harmonization & Transformations
Builds business-ready analytical marts:
- daily_sales
- customer_lifetime_value
- product_performance
- payment_summary
"""

import argparse
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    min as spark_min,
    max as spark_max,
    round as spark_round,
    to_date,
    current_timestamp,
    broadcast,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [TRANSFORM-GOLD] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_spark(app_name="GoldTransformations"):
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    if not minio_endpoint.startswith("http"):
        minio_endpoint = f"http://{minio_endpoint}"

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "admin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "password123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def build_daily_sales(orders_df, output_path):
    """Aggregate daily sales metrics."""
    logger.info("Building Gold mart: daily_sales...")
    df = (
        orders_df.filter(col("order_status") != "CANCELLED")
        .withColumn("order_date_dt", to_date(col("order_date")))
        .groupBy("order_date_dt", "order_status")
        .agg(
            count("order_id").alias("total_orders"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
        )
        .withColumn("calculated_at", current_timestamp())
    )
    df.write.mode("overwrite").format("parquet").save(f"{output_path}/daily_sales")
    logger.info(f"Gold daily_sales built: {df.count()} rows.")
    return df


def build_customer_lifetime_value(customers_df, orders_df, output_path):
    """Aggregate customer order frequency and lifetime value (CLV)."""
    logger.info("Building Gold mart: customer_lifetime_value...")
    order_agg = (
        orders_df.filter(col("order_status") != "CANCELLED")
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("lifetime_orders"),
            spark_round(spark_sum("total_amount"), 2).alias("lifetime_value"),
            spark_round(avg("total_amount"), 2).alias("average_basket_size"),
            spark_min("order_date").alias("first_order_date"),
            spark_max("order_date").alias("latest_order_date"),
        )
    )

    # Broadcast customer dimension table to demonstrate broadcast join
    clv_df = (
        customers_df.join(broadcast(order_agg), on="customer_id", how="left")
        .select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email"),
            col("city"),
            col("state"),
            col("lifetime_orders"),
            col("lifetime_value"),
            col("average_basket_size"),
            col("first_order_date"),
            col("latest_order_date"),
            current_timestamp().alias("calculated_at"),
        )
    )
    clv_df.write.mode("overwrite").format("parquet").save(f"{output_path}/customer_lifetime_value")
    logger.info(f"Gold customer_lifetime_value built: {clv_df.count()} rows.")
    return clv_df


def build_product_performance(products_df, order_items_df, output_path):
    """Aggregate product units sold, revenue, and category performance."""
    logger.info("Building Gold mart: product_performance...")
    items_agg = (
        order_items_df.groupBy("product_id")
        .agg(
            spark_sum("quantity").alias("total_units_sold"),
            spark_round(spark_sum(col("quantity") * col("unit_price")), 2).alias("gross_revenue"),
        )
    )

    perf_df = (
        products_df.join(items_agg, on="product_id", how="left")
        .select(
            col("product_id"),
            col("product_name"),
            col("category"),
            col("price"),
            col("stock_quantity"),
            col("total_units_sold"),
            col("gross_revenue"),
            current_timestamp().alias("calculated_at"),
        )
    )
    perf_df.write.mode("overwrite").format("parquet").save(f"{output_path}/product_performance")
    logger.info(f"Gold product_performance built: {perf_df.count()} rows.")
    return perf_df


def build_payment_summary(payments_df, output_path):
    """Aggregate payment method success rates and volume."""
    logger.info("Building Gold mart: payment_summary...")
    summary_df = (
        payments_df.groupBy("payment_method", "payment_status")
        .agg(
            count("payment_id").alias("transaction_count"),
            spark_round(spark_sum("payment_amount"), 2).alias("total_amount"),
            spark_round(avg("payment_amount"), 2).alias("avg_payment_amount"),
        )
        .withColumn("calculated_at", current_timestamp())
    )
    summary_df.write.mode("overwrite").format("parquet").save(f"{output_path}/payment_summary")
    logger.info(f"Gold payment_summary built: {summary_df.count()} rows.")
    return summary_df


def run_transformations(silver_base="data/silver", gold_base="data/gold"):
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Reading Silver tables from {silver_base}...")
    customers = spark.read.parquet(f"{silver_base}/customers")
    products = spark.read.parquet(f"{silver_base}/products")
    orders = spark.read.parquet(f"{silver_base}/orders")
    order_items = spark.read.parquet(f"{silver_base}/order_items")
    payments = spark.read.parquet(f"{silver_base}/payments")

    build_daily_sales(orders, gold_base)
    build_customer_lifetime_value(customers, orders, gold_base)
    build_product_performance(products, order_items, gold_base)
    build_payment_summary(payments, gold_base)
    logger.info("All Gold analytical marts created successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-base", default="data/silver")
    parser.add_argument("--gold-base", default="data/gold")
    args = parser.parse_args()
    run_transformations(args.silver_base, args.gold_base)
