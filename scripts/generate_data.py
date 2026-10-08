"""
Synthetic E-Commerce Data Generator
Generates realistic historical datasets for PostgreSQL seeding and batch ETL ingestion.
Supports output to PostgreSQL directly or export to CSV / Parquet format.
"""

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import random
import sys
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [DATA-GEN] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

fake = Faker("en_IN")  # Realistic Indian demographic data

PRODUCT_CATEGORIES = [
    ("Electronics", ["Wireless Earbuds", "Smartwatch", "Mechanical Keyboard", "USB-C Hub", "Bluetooth Speaker"]),
    ("Furniture", ["Ergonomic Desk Chair", "Adjustable Standing Desk", "Monitor Arm", "Footrest Cushion"]),
    ("Apparel", ["Cotton T-Shirt", "Slim Fit Denim", "Running Shoes", "Winter Hoodie", "Sports Cap"]),
    ("Home & Kitchen", ["Stainless Steel Flask", "Air Fryer", "Coffee Grinder", "Aroma Diffuser"]),
    ("Fitness", ["Yoga Mat", "Adjustable Dumbbells", "Resistance Bands", "Hydration Pack"]),
]

ORDER_STATUSES = ["PLACED", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "UPI", "NET_BANKING", "WALLET"]
PAYMENT_STATUSES = ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "PENDING", "FAILED"]


def generate_customers(count: int, start_id: int = 1):
    customers = []
    for i in range(count):
        cust_id = start_id + i
        created_days_ago = random.randint(1, 180)
        created_at = datetime.now(timezone.utc) - timedelta(days=created_days_ago)
        customers.append({
            "customer_id": cust_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": f"user_{cust_id}_{fake.user_name()}@{fake.free_email_domain()}",
            "city": fake.city(),
            "state": fake.state(),
            "created_at": created_at.isoformat(),
            "updated_at": created_at.isoformat(),
        })
    return customers


def generate_products(count: int, start_id: int = 1):
    products = []
    for i in range(count):
        prod_id = start_id + i
        category, items = random.choice(PRODUCT_CATEGORIES)
        item_base = random.choice(items)
        price = round(random.uniform(299.0, 49999.0), 2)
        stock = random.randint(10, 1000)
        created_at = datetime.now(timezone.utc) - timedelta(days=random.randint(60, 240))
        products.append({
            "product_id": prod_id,
            "product_name": f"{item_base} Model-{prod_id}",
            "category": category,
            "price": price,
            "stock_quantity": stock,
            "created_at": created_at.isoformat(),
            "updated_at": created_at.isoformat(),
        })
    return products


def generate_orders_and_items(order_count: int, customer_ids: list, products: list, start_order_id: int = 1001):
    orders = []
    order_items = []
    payments = []

    prod_map = {p["product_id"]: p for p in products}
    item_id = 1
    payment_id = 1

    for i in range(order_count):
        ord_id = start_order_id + i
        cust_id = random.choice(customer_ids)
        order_days_ago = random.randint(0, 90)
        ord_date = datetime.now(timezone.utc) - timedelta(days=order_days_ago, minutes=random.randint(0, 1440))
        
        # 1 to 4 items per order
        num_items = random.randint(1, 4)
        chosen_prods = random.sample(products, k=min(num_items, len(products)))
        
        total_amount = 0.0
        for prod in chosen_prods:
            qty = random.randint(1, 3)
            unit_price = prod["price"]
            total_amount += qty * unit_price
            order_items.append({
                "order_item_id": item_id,
                "order_id": ord_id,
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": unit_price,
            })
            item_id += 1

        total_amount = round(total_amount, 2)
        status = random.choice(ORDER_STATUSES)
        orders.append({
            "order_id": ord_id,
            "customer_id": cust_id,
            "order_status": status,
            "total_amount": total_amount,
            "order_date": ord_date.isoformat(),
            "updated_at": ord_date.isoformat(),
        })

        # Payment for order
        pay_status = "FAILED" if status == "CANCELLED" else random.choice(PAYMENT_STATUSES)
        payments.append({
            "payment_id": payment_id,
            "order_id": ord_id,
            "payment_method": random.choice(PAYMENT_METHODS),
            "payment_status": pay_status,
            "payment_amount": total_amount,
            "payment_date": ord_date.isoformat(),
        })
        payment_id += 1

    return orders, order_items, payments


def seed_postgres(host, port, dbname, user, password, customers, products, orders, order_items, payments):
    """Seed data directly into PostgreSQL using psycopg2."""
    import psycopg2
    from psycopg2.extras import execute_values

    logger.info(f"Connecting to PostgreSQL at {host}:{port}/{dbname}...")
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    cur = conn.cursor()

    logger.info(f"Inserting {len(customers)} customers...")
    cust_vals = [
        (c["customer_id"], c["first_name"], c["last_name"], c["email"], c["city"], c["state"], c["created_at"], c["updated_at"])
        for c in customers
    ]
    execute_values(
        cur,
        """INSERT INTO customers (customer_id, first_name, last_name, email, city, state, created_at, updated_at)
           VALUES %s ON CONFLICT (customer_id) DO UPDATE SET updated_at = EXCLUDED.updated_at""",
        cust_vals
    )

    logger.info(f"Inserting {len(products)} products...")
    prod_vals = [
        (p["product_id"], p["product_name"], p["category"], p["price"], p["stock_quantity"], p["created_at"], p["updated_at"])
        for p in products
    ]
    execute_values(
        cur,
        """INSERT INTO products (product_id, product_name, category, price, stock_quantity, created_at, updated_at)
           VALUES %s ON CONFLICT (product_id) DO UPDATE SET price = EXCLUDED.price""",
        prod_vals
    )

    logger.info(f"Inserting {len(orders)} orders...")
    ord_vals = [
        (o["order_id"], o["customer_id"], o["order_status"], o["total_amount"], o["order_date"], o["updated_at"])
        for o in orders
    ]
    execute_values(
        cur,
        """INSERT INTO orders (order_id, customer_id, order_status, total_amount, order_date, updated_at)
           VALUES %s ON CONFLICT (order_id) DO UPDATE SET order_status = EXCLUDED.order_status""",
        ord_vals
    )

    logger.info(f"Inserting {len(order_items)} order items...")
    item_vals = [
        (it["order_item_id"], it["order_id"], it["product_id"], it["quantity"], it["unit_price"])
        for it in order_items
    ]
    execute_values(
        cur,
        """INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price)
           VALUES %s ON CONFLICT (order_item_id) DO NOTHING""",
        item_vals
    )

    logger.info(f"Inserting {len(payments)} payments...")
    pay_vals = [
        (p["payment_id"], p["order_id"], p["payment_method"], p["payment_status"], p["payment_amount"], p["payment_date"])
        for p in payments
    ]
    execute_values(
        cur,
        """INSERT INTO payments (payment_id, order_id, payment_method, payment_status, payment_amount, payment_date)
           VALUES %s ON CONFLICT (payment_id) DO UPDATE SET payment_status = EXCLUDED.payment_status""",
        pay_vals
    )

    # Reset Postgres serial sequences
    cur.execute("SELECT setval('customers_customer_id_seq', (SELECT COALESCE(MAX(customer_id), 1) FROM customers));")
    cur.execute("SELECT setval('products_product_id_seq', (SELECT COALESCE(MAX(product_id), 1) FROM products));")
    cur.execute("SELECT setval('orders_order_id_seq', (SELECT COALESCE(MAX(order_id), 1) FROM orders));")
    cur.execute("SELECT setval('order_items_order_item_id_seq', (SELECT COALESCE(MAX(order_item_id), 1) FROM order_items));")
    cur.execute("SELECT setval('payments_payment_id_seq', (SELECT COALESCE(MAX(payment_id), 1) FROM payments));")

    conn.commit()
    cur.close()
    conn.close()
    logger.info("PostgreSQL seeding completed successfully!")


def export_to_csv_and_parquet(output_dir: str, customers, products, orders, order_items, payments):
    """Export generated tables to CSV and Parquet files in data/ for batch ETL."""
    import pandas as pd

    os.makedirs(output_dir, exist_ok=True)
    datasets = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
    }

    for name, data in datasets.items():
        df = pd.DataFrame(data)
        csv_path = os.path.join(output_dir, f"{name}.csv")
        parquet_path = os.path.join(output_dir, f"{name}.parquet")
        df.to_csv(csv_path, index=False)
        df.to_parquet(parquet_path, index=False)
        logger.info(f"Exported {name}: {len(df)} rows -> {csv_path} and {parquet_path}")


def main():
    parser = argparse.ArgumentParser(description="Synthetic E-Commerce Data Generator")
    parser.add_argument("--customers", type=int, default=100, help="Number of customers")
    parser.add_argument("--products", type=int, default=50, help="Number of products")
    parser.add_argument("--orders", type=int, default=500, help="Number of orders")
    parser.add_argument("--seed-db", action="store_true", help="Seed records into PostgreSQL")
    parser.add_argument("--export-local", action="store_true", help="Export to data/batch/ directory")
    parser.add_argument("--output-dir", type=str, default="data/batch", help="Output directory for exports")
    args = parser.parse_args()

    logger.info(f"Generating {args.customers} customers, {args.products} products, {args.orders} orders...")
    customers = generate_customers(args.customers, start_id=11)
    products = generate_products(args.products, start_id=11)
    cust_ids = [c["customer_id"] for c in customers] + list(range(1, 11))
    orders, order_items, payments = generate_orders_and_items(args.orders, cust_ids, products, start_order_id=1011)

    if args.export_local or not args.seed_db:
        export_to_csv_and_parquet(args.output_dir, customers, products, orders, order_items, payments)

    if args.seed_db:
        pg_host = os.getenv("POSTGRES_HOST", "localhost")
        pg_port = int(os.getenv("POSTGRES_PORT", "5432"))
        pg_db = os.getenv("POSTGRES_DB", "ecommerce")
        pg_user = os.getenv("POSTGRES_USER", "admin")
        pg_pwd = os.getenv("POSTGRES_PASSWORD", "change_me")
        seed_postgres(pg_host, pg_port, pg_db, pg_user, pg_pwd, customers, products, orders, order_items, payments)


if __name__ == "__main__":
    main()
