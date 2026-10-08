"""
Kafka Topics Provisioning Script
Idempotently creates required event topics with partition and retention policies.
"""

import logging
import os
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [KAFKA-TOPICS] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

TOPICS_TO_CREATE = [
    "ecommerce.public.customers",
    "ecommerce.public.products",
    "ecommerce.public.orders",
    "ecommerce.public.order_items",
    "ecommerce.public.payments",
    "ecommerce.dlq.errors",
    "ecommerce.batch.events",
]


def create_topics_via_kafka_python(bootstrap_servers: str):
    """Attempt topic creation using kafka-python KafkaAdminClient."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    logger.info(f"Connecting to Kafka Admin at {bootstrap_servers}...")
    admin_client = None
    retries = 15
    for attempt in range(retries):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="cdc-topic-provisioner",
                request_timeout_ms=10000,
            )
            logger.info("Connected to Kafka broker.")
            break
        except Exception as e:
            logger.warning(f"Connection attempt {attempt + 1}/{retries} failed: {e}. Retrying in 2s...")
            time.sleep(2)

    if not admin_client:
        logger.error("Could not connect to Kafka Admin.")
        return False

    existing_topics = set(admin_client.list_topics())
    logger.info(f"Existing topics: {list(existing_topics)}")

    new_topics = []
    for topic_name in TOPICS_TO_CREATE:
        if topic_name not in existing_topics:
            new_topics.append(
                NewTopic(
                    name=topic_name,
                    num_partitions=3,
                    replication_factor=1,
                    topic_configs={"retention.ms": "604800000"},  # 7 days
                )
            )
        else:
            logger.info(f"Topic '{topic_name}' already exists.")

    if new_topics:
        try:
            admin_client.create_topics(new_topics=new_topics, validate_only=False)
            for t in new_topics:
                logger.info(f"Successfully created topic: {t.name} (partitions: {t.num_partitions})")
        except TopicAlreadyExistsError:
            pass
        except Exception as e:
            logger.error(f"Error creating topics: {e}")
            return False

    admin_client.close()
    return True


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    logger.info(f"Starting topic provisioning targeting {bootstrap_servers}...")
    try:
        success = create_topics_via_kafka_python(bootstrap_servers)
        if success:
            logger.info("All Kafka topics verified/created successfully!")
        else:
            logger.error("Failed to provision topics.")
            sys.exit(1)
    except ImportError:
        logger.warning("kafka-python not installed. Run 'pip install -r requirements.txt'")
        sys.exit(1)


if __name__ == "__main__":
    main()
