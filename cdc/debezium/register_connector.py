"""
Debezium Connector Registration Script
Registers the PostgreSQL CDC connector with Kafka Connect and verifies task status.
"""

import json
import logging
import os
import sys
import time
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [CDC-REGISTRATION] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DEBEZIUM_URL = os.getenv("DEBEZIUM_CONNECT_URL", "http://localhost:8083")
CONNECTOR_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "connector.json")


def wait_for_debezium(url: str, timeout: int = 120) -> bool:
    """Poll Debezium Connect until REST endpoint is accessible."""
    logger.info(f"Checking Debezium Connect endpoint at {url}/connectors...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{url}/connectors", timeout=5)
            if response.status_code == 200:
                logger.info("Debezium Connect is ready.")
                return True
        except requests.exceptions.RequestException:
            pass
        logger.info("Waiting for Debezium Connect to start... (retrying in 3s)")
        time.sleep(3)
    logger.error("Timed out waiting for Debezium Connect.")
    return False


def register_connector(url: str, config_path: str) -> bool:
    """Register or update the Debezium Postgres connector."""
    if not os.path.exists(config_path):
        logger.error(f"Connector configuration file not found at {config_path}")
        return False

    with open(config_path, "r", encoding="utf-8") as f:
        connector_data = json.load(f)

    connector_name = connector_data["name"]
    connector_config = connector_data["config"]

    # Check if connector already exists
    check_url = f"{url}/connectors/{connector_name}"
    response = requests.get(check_url)

    if response.status_code == 200:
        logger.info(f"Connector '{connector_name}' already exists. Updating configuration...")
        put_url = f"{check_url}/config"
        res = requests.put(
            put_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config),
            timeout=10,
        )
    else:
        logger.info(f"Registering new connector '{connector_name}'...")
        post_url = f"{url}/connectors"
        res = requests.post(
            post_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_data),
            timeout=10,
        )

    if res.status_code in (200, 201):
        logger.info(f"Connector '{connector_name}' successfully configured!")
    else:
        logger.error(f"Failed to configure connector. Status: {res.status_code}, Response: {res.text}")
        return False

    # Verify connector and task status
    time.sleep(3)
    status_url = f"{url}/connectors/{connector_name}/status"
    status_res = requests.get(status_url, timeout=10)
    if status_res.status_code == 200:
        status_data = status_res.json()
        connector_state = status_data.get("connector", {}).get("state")
        tasks = status_data.get("tasks", [])
        logger.info(f"Connector state: {connector_state}")
        for task in tasks:
            logger.info(f"Task {task.get('id')} state: {task.get('state')}")
            if task.get("state") == "FAILED":
                logger.error(f"Task error trace: {task.get('trace')}")
                return False
        return True
    else:
        logger.warning(f"Could not retrieve connector status: {status_res.text}")
        return False


def main():
    logger.info("Starting Debezium PostgreSQL connector registration...")
    if not wait_for_debezium(DEBEZIUM_URL):
        sys.exit(1)

    if not register_connector(DEBEZIUM_URL, CONNECTOR_CONFIG_PATH):
        sys.exit(1)

    logger.info("Debezium CDC registration completed successfully!")


if __name__ == "__main__":
    main()
