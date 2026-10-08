"""
Live CDC & Streaming Event Simulator
Generates continuous or batch INSERT, UPDATE, DELETE, duplicate, and poisoned events.
Supports both database transactions (PostgreSQL WAL -> Debezium -> Kafka)
and direct Kafka topic publishing.
"""

import argparse
from datetime import datetime, timezone
import json
import logging
import os
import random
import sys
import time
import uuid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [EVENT-SIMULATOR] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ORDER_STATUS_FLOW = ["PLACED", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED"]


def simulate_postgres_events(host, port, dbname, user, password, iterations=10, delay=1.0, inject_errors=True):
    """Generates SQL INSERT/UPDATE/DELETE events in PostgreSQL to trigger Debezium CDC."""
    import psycopg2

    logger.info(f"Connecting to PostgreSQL at {host}:{port}/{dbname} for CDC simulation...")
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    conn.autocommit = True
    cur = conn.cursor()

    for i in range(1, iterations + 1):
        event_type = random.choice(["insert_order", "update_order", "insert_customer", "update_customer", "delete_test"])
        
        # 1. INSERT customer
        if event_type == "insert_customer":
            cust_name = f"TestUser_{random.randint(1000, 9999)}"
            email = f"{cust_name.lower()}@example.com"
            cur.execute(
                """INSERT INTO customers (first_name, last_name, email, city, state)
                   VALUES (%s, 'Tester', %s, 'Bengaluru', 'Karnataka') RETURNING customer_id;""",
                (cust_name, email)
            )
            cid = cur.fetchone()[0]
            logger.info(f"[{i}/{iterations}] CDC INSERT customer: id={cid}, email={email}")

        # 2. UPDATE customer
        elif event_type == "update_customer":
            cur.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1;")
            row = cur.fetchone()
            if row:
                cid = row[0]
                new_city = random.choice(["Bengaluru", "Mumbai", "Hyderabad", "Delhi", "Chennai", "Pune"])
                cur.execute(
                    "UPDATE customers SET city = %s, updated_at = CURRENT_TIMESTAMP WHERE customer_id = %s;",
                    (new_city, cid)
                )
                logger.info(f"[{i}/{iterations}] CDC UPDATE customer: id={cid} new_city={new_city}")

        # 3. INSERT order
        elif event_type == "insert_order":
            cur.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1;")
            crow = cur.fetchone()
            if crow:
                cid = crow[0]
                amount = round(random.uniform(500.0, 15000.0), 2)
                cur.execute(
                    """INSERT INTO orders (customer_id, order_status, total_amount)
                       VALUES (%s, 'PLACED', %s) RETURNING order_id;""",
                    (cid, amount)
                )
                oid = cur.fetchone()[0]
                # Also create a payment
                cur.execute(
                    """INSERT INTO payments (order_id, payment_method, payment_status, payment_amount)
                       VALUES (%s, 'UPI', 'SUCCESS', %s);""",
                    (oid, amount)
                )
                logger.info(f"[{i}/{iterations}] CDC INSERT order: id={oid}, customer={cid}, amount=Rs.{amount}")

        # 4. UPDATE order status
        elif event_type == "update_order":
            cur.execute("SELECT order_id, order_status FROM orders WHERE order_status != 'DELIVERED' ORDER BY RANDOM() LIMIT 1;")
            orow = cur.fetchone()
            if orow:
                oid, curr_status = orow
                if curr_status in ORDER_STATUS_FLOW:
                    curr_idx = ORDER_STATUS_FLOW.index(curr_status)
                    next_status = ORDER_STATUS_FLOW[min(curr_idx + 1, len(ORDER_STATUS_FLOW) - 1)]
                else:
                    next_status = "DELIVERED"
                cur.execute(
                    "UPDATE orders SET order_status = %s, updated_at = CURRENT_TIMESTAMP WHERE order_id = %s;",
                    (next_status, oid)
                )
                logger.info(f"[{i}/{iterations}] CDC UPDATE order: id={oid} status={curr_status}->{next_status}")

        # 5. DELETE test row
        elif event_type == "delete_test":
            cur.execute("SELECT customer_id FROM customers WHERE first_name LIKE 'TestUser_%' ORDER BY RANDOM() LIMIT 1;")
            drow = cur.fetchone()
            if drow:
                cid = drow[0]
                cur.execute("DELETE FROM customers WHERE customer_id = %s;", (cid,))
                logger.info(f"[{i}/{iterations}] CDC DELETE customer: id={cid}")

        time.sleep(delay)

    cur.close()
    conn.close()
    logger.info("PostgreSQL event simulation completed.")


def simulate_kafka_events(bootstrap_servers, iterations=10, delay=1.0, inject_dlq=True, inject_duplicates=True):
    """Produces Debezium-like envelopes directly into Kafka topics."""
    from kafka import KafkaProducer

    logger.info(f"Connecting KafkaProducer to {bootstrap_servers}...")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )

    topics = {
        "order": "ecommerce.public.orders",
        "customer": "ecommerce.public.customers",
        "payment": "ecommerce.public.payments",
    }

    last_order_event = None

    for i in range(1, iterations + 1):
        target = random.choice(["order", "customer", "payment"])

        if target == "order":
            ord_id = random.randint(5000, 9999)
            event = {
                "op": "c",
                "ts_ms": int(time.time() * 1000),
                "source": {"table": "orders", "db": "ecommerce"},
                "before": None,
                "after": {
                    "order_id": ord_id,
                    "customer_id": random.randint(1, 100),
                    "order_status": "PLACED",
                    "total_amount": round(random.uniform(200.0, 9999.0), 2),
                    "order_date": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            }

            # Inject intentional invalid record to test DLQ
            if inject_dlq and i % 5 == 0:
                event["after"]["total_amount"] = -500.00  # Negative amount violation!
                logger.warning(f"[{i}/{iterations}] INJECTING DLQ POISON EVENT: Negative total_amount in order {ord_id}")

            producer.send(topics["order"], key=ord_id, value=event)
            logger.info(f"[{i}/{iterations}] Produced Kafka event -> {topics['order']}: order_id={ord_id}")

            # Inject duplicate event to test deduplication
            if inject_duplicates and i % 6 == 0 and last_order_event:
                producer.send(topics["order"], key=last_order_event["after"]["order_id"], value=last_order_event)
                logger.warning(f"[{i}/{iterations}] INJECTING DUPLICATE EVENT: Re-sending order {last_order_event['after']['order_id']}")

            last_order_event = event

        elif target == "customer":
            cust_id = random.randint(200, 999)
            event = {
                "op": "u",
                "ts_ms": int(time.time() * 1000),
                "source": {"table": "customers", "db": "ecommerce"},
                "before": {"customer_id": cust_id, "city": "Mumbai"},
                "after": {
                    "customer_id": cust_id,
                    "first_name": "Simulated",
                    "last_name": "Customer",
                    "email": f"cust_{cust_id}@example.com",
                    "city": random.choice(["Bengaluru", "Delhi", "Chennai"]),
                    "state": "Karnataka",
                },
            }
            producer.send(topics["customer"], key=cust_id, value=event)
            logger.info(f"[{i}/{iterations}] Produced Kafka event -> {topics['customer']}: customer_id={cust_id}")

        elif target == "payment":
            pay_id = random.randint(10000, 19999)
            event = {
                "op": "c",
                "ts_ms": int(time.time() * 1000),
                "source": {"table": "payments", "db": "ecommerce"},
                "before": None,
                "after": {
                    "payment_id": pay_id,
                    "order_id": random.randint(1001, 2000),
                    "payment_method": random.choice(["UPI", "CREDIT_CARD", "WALLET"]),
                    "payment_status": "SUCCESS",
                    "payment_amount": round(random.uniform(100.0, 5000.0), 2),
                    "payment_date": datetime.now(timezone.utc).isoformat(),
                },
            }
            producer.send(topics["payment"], key=pay_id, value=event)
            logger.info(f"[{i}/{iterations}] Produced Kafka event -> {topics['payment']}: payment_id={pay_id}")

        producer.flush()
        time.sleep(delay)

    producer.close()
    logger.info("Direct Kafka event simulation completed.")


def main():
    parser = argparse.ArgumentParser(description="Live CDC and Streaming Event Simulator")
    parser.add_argument("--mode", choices=["postgres", "kafka"], default="postgres", help="Target mode for event simulation")
    parser.add_argument("--iterations", type=int, default=15, help="Number of event iterations to run")
    parser.add_argument("--delay", type=float, default=0.8, help="Delay in seconds between events")
    parser.add_argument("--continuous", action="store_true", help="Run indefinitely until stopped")
    parser.add_argument("--no-dlq", action="store_true", help="Do not inject DLQ error records")
    parser.add_argument("--no-duplicates", action="store_true", help="Do not inject duplicate records")
    args = parser.parse_args()

    while True:
        if args.mode == "postgres":
            simulate_postgres_events(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                dbname=os.getenv("POSTGRES_DB", "ecommerce"),
                user=os.getenv("POSTGRES_USER", "admin"),
                password=os.getenv("POSTGRES_PASSWORD", "change_me"),
                iterations=args.iterations,
                delay=args.delay,
            )
        else:
            simulate_kafka_events(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
                iterations=args.iterations,
                delay=args.delay,
                inject_dlq=not args.no_dlq,
                inject_duplicates=not args.no_duplicates,
            )

        if not args.continuous:
            break
        logger.info("Repeating simulation batch...")
        time.sleep(1)


if __name__ == "__main__":
    main()
