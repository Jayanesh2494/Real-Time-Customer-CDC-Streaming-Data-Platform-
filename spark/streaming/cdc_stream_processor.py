"""
Universal PySpark CDC Stream Processor
Consumes Debezium CDC events from Kafka, validates schema & data quality,
deduplicates messages, and routes valid records to Silver Parquet on MinIO
while routing invalid records to the Dead-Letter Queue (DLQ).
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    when,
    current_timestamp,
    to_date,
    expr,
    coalesce,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    IntegerType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SPARK-STREAM] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def create_spark_session(app_name="CDCStreamProcessor"):
    """Initialize SparkSession with S3A MinIO and Kafka dependencies."""
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    if not minio_endpoint.startswith("http"):
        minio_endpoint = f"http://{minio_endpoint}"

    access_key = os.getenv("MINIO_ROOT_USER", "admin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password123")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.streaming.checkpointLocation.cleanup", "false")
        .config("spark.sql.shuffle.partitions", "2")
    )
    return builder.getOrCreate()


# Schemas for Debezium CDC Payloads
ORDER_PAYLOAD_SCHEMA = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_status", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("updated_at", StringType(), True),
])

CUSTOMER_PAYLOAD_SCHEMA = StructType([
    StructField("customer_id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

PAYMENT_PAYLOAD_SCHEMA = StructType([
    StructField("payment_id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_amount", DoubleType(), True),
    StructField("payment_date", StringType(), True),
])


def get_debezium_envelope_schema(payload_schema):
    """Wrap payload schema in standard Debezium envelope."""
    return StructType([
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("before", payload_schema, True),
        StructField("after", payload_schema, True),
    ])


def process_orders_stream(spark, kafka_bootstrap, output_base_path, checkpoint_base_path):
    """Consume and process the orders CDC stream."""
    logger.info("Initializing Orders CDC streaming pipeline...")

    envelope_schema = get_debezium_envelope_schema(ORDER_PAYLOAD_SCHEMA)

    # 1. Read Kafka Stream
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "ecommerce.public.orders")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON CDC Envelope
    parsed = (
        raw_stream.selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_value")
        .select(
            col("kafka_key"),
            from_json(col("json_value"), envelope_schema).alias("cdc"),
            current_timestamp().alias("processing_time"),
        )
        .select(
            col("cdc.op").alias("operation"),
            col("cdc.ts_ms").alias("event_ts_ms"),
            # For deletes ('d'), take 'before'; for creates/updates, take 'after'
            when(col("cdc.op") == "d", col("cdc.before")).otherwise(col("cdc.after")).alias("data"),
            col("processing_time"),
        )
    )

    # 3. Flatten and Deduplicate
    flattened = (
        parsed.select(
            col("operation"),
            col("event_ts_ms"),
            col("data.order_id").alias("order_id"),
            col("data.customer_id").alias("customer_id"),
            col("data.order_status").alias("order_status"),
            col("data.total_amount").alias("total_amount"),
            col("data.order_date").alias("order_date"),
            col("data.updated_at").alias("updated_at"),
            col("processing_time"),
        )
        .filter(col("order_id").isNotNull())
        .dropDuplicates(["order_id", "operation", "event_ts_ms"])
    )

    # 4. Data Quality Validation Rules
    allowed_statuses = "('PLACED', 'CONFIRMED', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'REFUNDED')"
    
    validated = (
        flattened.withColumn(
            "validation_status",
            when(col("customer_id").isNull(), lit("INVALID"))
            .when(col("total_amount") < 0.0, lit("INVALID"))
            .when(expr(f"order_status NOT IN {allowed_statuses}"), lit("INVALID"))
            .otherwise(lit("VALID")),
        )
        .withColumn(
            "validation_reason",
            when(col("customer_id").isNull(), lit("customer_id is null"))
            .when(col("total_amount") < 0.0, lit("total_amount is negative"))
            .when(expr(f"order_status NOT IN {allowed_statuses}"), lit("invalid order_status enum"))
            .otherwise(lit(None).cast(StringType())),
        )
        .withColumn("order_date_dt", to_date(col("order_date")))
    )

    # 5. Split Streams into Valid (Silver) vs Invalid (DLQ)
    valid_records = validated.filter(col("validation_status") == "VALID")
    invalid_records = validated.filter(col("validation_status") == "INVALID")

    # 6. Stream sinks
    silver_query = (
        valid_records.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("order_status")
        .option("path", f"{output_base_path}/silver/orders")
        .option("checkpointLocation", f"{checkpoint_base_path}/orders_silver")
        .start()
    )

    dlq_query = (
        invalid_records.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"{output_base_path}/errors/orders")
        .option("checkpointLocation", f"{checkpoint_base_path}/orders_dlq")
        .start()
    )

    logger.info("Order streaming queries started.")
    return [silver_query, dlq_query]


def main():
    kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    data_lake_path = os.getenv("DATA_LAKE_PATH", "s3a://ecommerce-data")
    checkpoint_dir = os.getenv("CHECKPOINT_PATH", "s3a://ecommerce-data/checkpoints")

    logger.info(f"Targeting Kafka: {kafka_server}")
    logger.info(f"Targeting Lake Path: {data_lake_path}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    queries = process_orders_stream(spark, kafka_server, data_lake_path, checkpoint_dir)

    # Await streaming termination
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
