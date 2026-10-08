"""
PySpark Dedicated Customer Stream Processor
Consumes customer CDC events, validates email integrity and mandatory fields,
and writes to MinIO Silver Parquet and Error DLQ.
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when, current_timestamp
from pyspark.sql.types import StringType
from cdc_stream_processor import (
    create_spark_session,
    get_debezium_envelope_schema,
    CUSTOMER_PAYLOAD_SCHEMA,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [CUSTOMER-STREAM] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def process_customer_stream(spark, kafka_bootstrap, output_base_path, checkpoint_base_path):
    logger.info("Initializing Customer CDC streaming pipeline...")
    envelope_schema = get_debezium_envelope_schema(CUSTOMER_PAYLOAD_SCHEMA)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "ecommerce.public.customers")
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
            col("data.customer_id").alias("customer_id"),
            col("data.first_name").alias("first_name"),
            col("data.last_name").alias("last_name"),
            col("data.email").alias("email"),
            col("data.city").alias("city"),
            col("data.state").alias("state"),
            col("data.created_at").alias("created_at"),
            col("data.updated_at").alias("updated_at"),
            col("processing_time"),
        )
        .filter(col("customer_id").isNotNull())
        .dropDuplicates(["customer_id", "operation", "event_ts_ms"])
    )

    # Email regex validation
    email_pattern = "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
    validated = (
        flattened.withColumn(
            "validation_status",
            when(col("email").isNull() | ~col("email").rlike(email_pattern), lit("INVALID"))
            .when(col("first_name").isNull() | (col("first_name") == ""), lit("INVALID"))
            .otherwise(lit("VALID")),
        )
        .withColumn(
            "validation_reason",
            when(col("email").isNull() | ~col("email").rlike(email_pattern), lit("invalid email format"))
            .when(col("first_name").isNull() | (col("first_name") == ""), lit("missing first_name"))
            .otherwise(lit(None).cast(StringType())),
        )
    )

    valid_records = validated.filter(col("validation_status") == "VALID")
    invalid_records = validated.filter(col("validation_status") == "INVALID")

    silver_query = (
        valid_records.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("state")
        .option("path", f"{output_base_path}/silver/customers")
        .option("checkpointLocation", f"{checkpoint_base_path}/customers_silver")
        .start()
    )

    dlq_query = (
        invalid_records.writeStream.format("parquet")
        .outputMode("append")
        .option("path", f"{output_base_path}/errors/customers")
        .option("checkpointLocation", f"{checkpoint_base_path}/customers_dlq")
        .start()
    )

    logger.info("Customer streaming queries started.")
    return [silver_query, dlq_query]


def main():
    kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    data_lake_path = os.getenv("DATA_LAKE_PATH", "s3a://ecommerce-data")
    checkpoint_dir = os.getenv("CHECKPOINT_PATH", "s3a://ecommerce-data/checkpoints")

    spark = create_spark_session(app_name="CustomerStreamProcessor")
    spark.sparkContext.setLogLevel("WARN")

    queries = process_customer_stream(spark, kafka_server, data_lake_path, checkpoint_dir)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
