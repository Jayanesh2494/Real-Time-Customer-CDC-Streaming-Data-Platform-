"""
MinIO Object Storage Bucket Provisioner
Initializes 'ecommerce-data' bucket and Medallion Lake directory prefixes.
"""

import os
import sys
import logging
import time
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [MINIO-INIT] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def setup_minio_bucket(endpoint="localhost:9000", access_key="admin", secret_key="password123", bucket_name="ecommerce-data"):
    logger.info(f"Connecting to MinIO endpoint at {endpoint}...")
    
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False,
    )

    # Wait for MinIO service readiness
    retries = 15
    for i in range(retries):
        try:
            client.list_buckets()
            logger.info("MinIO connection established.")
            break
        except Exception as e:
            logger.info(f"Waiting for MinIO service... ({i+1}/{retries})")
            time.sleep(2)
    else:
        logger.error("Could not reach MinIO service.")
        return False

    # Check and create bucket
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            logger.info(f"Bucket '{bucket_name}' already exists.")

        # Create Medallion directories with empty .keep objects
        tiers = ["bronze", "silver", "gold", "errors", "checkpoints"]
        from io import BytesIO
        for tier in tiers:
            obj_name = f"{tier}/.keep"
            client.put_object(bucket_name, obj_name, BytesIO(b""), 0)
            logger.info(f"Initialized tier: {tier}/")

        return True
    except S3Error as err:
        logger.error(f"S3 Error configuring bucket: {err}")
        return False


def main():
    endpoint = os.getenv("MINIO_HOST_ENDPOINT", "localhost:9000")
    user = os.getenv("MINIO_ROOT_USER", "admin")
    pwd = os.getenv("MINIO_ROOT_PASSWORD", "password123")
    bucket = os.getenv("MINIO_BUCKET", "ecommerce-data")

    if setup_minio_bucket(endpoint, user, pwd, bucket):
        logger.info("MinIO Data Lake storage setup completed successfully!")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
