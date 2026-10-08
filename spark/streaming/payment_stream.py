"""
PySpark Dedicated Payment Stream Processor
Consumes payment CDC events, validates amounts, methods, and statuses,
and writes to MinIO Silver Parquet and Error DLQ.
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when, current_timestamp, expr
from pyspark.sql.types import StringType
from cdc_stream_processor import (
    create_spark_session,
    get_debezium_envelope_schema,
    PAYMENT_PAYLOAD_SCHEMA,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PAYMENT-STREAM] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def process_payment_stream(spark, kafka_bootstrap, output_base_path, checkpoint_base_path):
    logger.info("Initializing Payment CDC streaming pipeline...")
    envelope_schema = get_debezium_envelope_schema(PAYMENT_PAYLOAD_SCHEMA)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "ecommerce.public.payments")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

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
            when(col("cdc.op") == "d", col("cdc.before")).otherwise(col("cdc.after")).alias("data"),
            col("processing_time"),
        )
    )

    flattened = (
        parsed.select(
            col("operation"),
            col("event_ts_ms"),
            col("data.payment_id").alias("payment_id"),
            col("data.order_id").alias("order_id"),
            col("data.payment_method").alias("payment_method"),
            col("data.payment_status").alias("payment_status"),
            col("data.payment_amount").alias("payment_amount"),
            col("data.payment_date").alias("payment_date"),
            col("processing_time"),
        )
        .filter(col("payment_id").isNotNull())
        .dropDuplicates(["payment_id", "operation", "event_ts_ms"])
    )

    allowed_methods = "('CREDIT_CARD', 'DEBIT_CARD', 'UPI', 'NET_BANKING', 'WALLET', 'COD')"
    allowed_statuses = "('SUCCESS', 'PENDING', 'FAILED', 'REFUNDED')"

    validated = (
        flattened.withColumn(
            "validation_status",
            when(col("payment_amount") < 0.0, lit("INVALID"))
            .when(expr(f"payment_method NOT IN {allowed_methods}"), lit("INVALID"))
            .when(expr(f"payment_status NOT IN {allowed_statuses}"), lit("INVALID"))
            .otherwise(lit("VALID")),
        )
        .withColumn(
            "validation_reason",
            when(col("payment_amount") < 0.0, lit("negative payment_amount"))
            .when(expr(f"payment_method NOT IN {allowed_methods}"), lit("invalid payment_method"))
            .when(expr(f"payment_status NOT IN {allowed_statuses}"), lit("invalid payment_status"))
            .otherwise(lit(None).cast(StringType())),
        )
    )

    valid_records = validated.filter(col("validation_status") == "VALID")
    invalid_records = validated.filter(col("validation_status") == "INVALID")

    silver_query = (
        valid_records.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("payment_method")
        .option("path", f"{output_base_path}/silver/payments")
        .option("checkpointLocation", f"{checkpoint_base_path}/payments_silver")
        .start()
    )

    dlq_query = (
        invalid_records.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"{output_base_path}/errors/payments")
        .option("checkpointLocation", f"{checkpoint_base_path}/payments_dlq")
        .start()
    )

    logger.info("Payment streaming queries started.")
    return [silver_query, dlq_query]


def main():
    kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    data_lake_path = os.getenv("DATA_LAKE_PATH", "s3a://ecommerce-data")
    checkpoint_dir = os.getenv("CHECKPOINT_PATH", "s3a://ecommerce-data/checkpoints")

    spark = create_spark_session(app_name="PaymentStreamProcessor")
    spark.sparkContext.setLogLevel("WARN")

    queries = process_payment_stream(spark, kafka_server, data_lake_path, checkpoint_dir)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
