"""
PySpark Dedicated Order Stream Processor
Processes order events, validates against e-commerce business rules,
and writes to MinIO Silver Parquet and Error DLQ.
"""

import os
import sys
from cdc_stream_processor import create_spark_session, process_orders_stream


def main():
    kafka_server = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    data_lake_path = os.getenv("DATA_LAKE_PATH", "s3a://ecommerce-data")
    checkpoint_dir = os.getenv("CHECKPOINT_PATH", "s3a://ecommerce-data/checkpoints")

    spark = create_spark_session(app_name="OrderStreamProcessor")
    spark.sparkContext.setLogLevel("WARN")

    queries = process_orders_stream(spark, kafka_server, data_lake_path, checkpoint_dir)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
