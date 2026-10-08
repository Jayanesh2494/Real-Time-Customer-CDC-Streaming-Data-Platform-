"""
PySpark Batch Ingestion Pipeline
Reads historical batch files (CSV/Parquet), applies validation & deduplication,
and loads curated records into the Silver layer of the Data Lake.
"""

import argparse
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when, to_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [BATCH-INGEST] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_spark(app_name="BatchIngestionPipeline"):
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


def ingest_table(spark, input_dir, output_base, table_name, partition_cols=None):
    logger.info(f"Ingesting table '{table_name}' from {input_dir}...")
    csv_file = os.path.join(input_dir, f"{table_name}.csv")
    parquet_file = os.path.join(input_dir, f"{table_name}.parquet")

    if os.path.exists(parquet_file):
        df = spark.read.parquet(parquet_file)
    elif os.path.exists(csv_file):
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file)
    else:
        logger.warning(f"No source data found for {table_name} at {input_dir}")
        return None

    # Deduplicate based on primary key
    pk_map = {
        "customers": "customer_id",
        "products": "product_id",
        "orders": "order_id",
        "order_items": "order_item_id",
        "payments": "payment_id",
    }
    pk = pk_map.get(table_name)
    if pk and pk in df.columns:
        df = df.filter(col(pk).isNotNull()).dropDuplicates([pk])

    # Enrich metadata
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # Write to Silver layer
    target_path = f"{output_base}/silver/{table_name}"
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols and all(p in df.columns for p in partition_cols):
        writer = writer.partitionBy(*partition_cols)

    writer.save(target_path)
    count = df.count()
    logger.info(f"Successfully loaded {count} records into Silver layer at {target_path}")
    return count


def run_batch_ingestion(input_dir="data/batch", output_base="data"):
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    tables = [
        ("customers", ["state"]),
        ("products", ["category"]),
        ("orders", ["order_status"]),
        ("order_items", None),
        ("payments", ["payment_method"]),
    ]

    metrics = {}
    for table, partition_cols in tables:
        count = ingest_table(spark, input_dir, output_base, table, partition_cols)
        metrics[table] = count

    logger.info(f"Batch ingestion summary: {metrics}")
    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/batch")
    parser.add_argument("--output-base", default="data")
    args = parser.parse_args()
    run_batch_ingestion(args.input_dir, args.output_base)
