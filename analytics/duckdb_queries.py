"""
DuckDB Analytical Engine Runner
Executes fast analytical SQL queries over MinIO S3 Parquet or local Data Lake Parquet files.
Demonstrates predicate pushdown, columnar scans, and sub-second analytical aggregations.
"""

import argparse
import glob
import logging
import os
import time
import duckdb
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [DUCKDB] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")


def init_duckdb_connection(use_minio=False):
    """Initializes DuckDB and configures S3/MinIO HTTPFS extension if requested."""
    conn = duckdb.connect(database=":memory:")

    if use_minio:
        logger.info("Configuring DuckDB S3/MinIO HTTPFS extension...")
        minio_host = os.getenv("MINIO_HOST", "localhost")
        minio_port = os.getenv("MINIO_PORT", "9000")
        access_key = os.getenv("MINIO_ROOT_USER", "admin")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password123")

        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(f"SET s3_endpoint='{minio_host}:{minio_port}';")
        conn.execute(f"SET s3_access_key_id='{access_key}';")
        conn.execute(f"SET s3_secret_access_key='{secret_key}';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")

    return conn


def register_views(conn, data_dir="data", use_minio=False):
    """Registers SQL views pointing to Parquet datasets."""
    tables = ["customers", "products", "orders", "order_items", "payments"]

    for table in tables:
        if use_minio:
            path = f"s3://ecommerce-data/silver/{table}/**/*.parquet"
        else:
            # Check Silver, Batch, or fallback
            silver_path = os.path.join(data_dir, "silver", table, "**", "*.parquet")
            batch_path = os.path.join(data_dir, "batch", f"{table}.parquet")
            csv_path = os.path.join(data_dir, "batch", f"{table}.csv")

            if glob.glob(silver_path, recursive=True):
                path = silver_path.replace("\\", "/")
            elif os.path.exists(batch_path):
                path = batch_path.replace("\\", "/")
            elif os.path.exists(csv_path):
                path = csv_path.replace("\\", "/")
            else:
                path = None

        if path:
            try:
                if path.endswith(".csv"):
                    conn.execute(f"CREATE OR REPLACE VIEW {table}_source AS SELECT * FROM read_csv_auto('{path}');")
                else:
                    conn.execute(f"CREATE OR REPLACE VIEW {table}_source AS SELECT * FROM read_parquet('{path}');")
                logger.info(f"Registered view '{table}_source' -> {path}")
            except Exception as e:
                logger.warning(f"Could not register view {table}: {e}")

    # Register error DLQ view
    error_path = os.path.join(data_dir, "errors", "**", "*.parquet").replace("\\", "/")
    if glob.glob(os.path.join(data_dir, "errors", "**", "*.parquet"), recursive=True):
        conn.execute(f"CREATE OR REPLACE VIEW errors_source AS SELECT * FROM read_parquet('{error_path}');")
    else:
        # Mock empty errors view for schema conformity
        conn.execute("""
            CREATE OR REPLACE VIEW errors_source AS 
            SELECT 'orders' AS source_table, 'INVALID' AS validation_status, 
                   'total_amount is negative' AS validation_reason, 
                   CURRENT_TIMESTAMP AS processing_timestamp
            LIMIT 0;
        """)


def run_query(conn, query_name):
    """Reads SQL file, executes it, measures runtime, and prints formatted output."""
    file_path = os.path.join(SQL_DIR, f"{query_name}.sql")
    if not os.path.exists(file_path):
        logger.error(f"SQL file not found: {file_path}")
        return

    with open(file_path, "r", encoding="utf-8") as f:
        query_sql = f.read()

    logger.info(f"\n========================================================")
    logger.info(f"Executing Analytical Query: {query_name}")
    logger.info(f"========================================================")

    start_time = time.time()
    try:
        result = conn.execute(query_sql).fetchall()
        columns = [desc[0] for desc in conn.description]
        elapsed_ms = (time.time() - start_time) * 1000

        print("\n" + tabulate(result, headers=columns, tablefmt="psql"))
        logger.info(f"Query returned {len(result)} rows in {elapsed_ms:.2f} ms\n")
    except Exception as e:
        logger.error(f"Error executing query {query_name}: {e}")


def main():
    parser = argparse.ArgumentParser(description="DuckDB Analytical Runner")
    parser.add_argument("--query", choices=["daily_sales", "customer_lifetime_value", "product_performance", "payment_summary", "operations_audit", "all"], default="all")
    parser.add_argument("--use-minio", action="store_true", help="Query MinIO S3 object storage directly")
    parser.add_argument("--data-dir", default="data", help="Local data directory")
    args = parser.parse_args()

    conn = init_duckdb_connection(args.use_minio)
    register_views(conn, args.data_dir, args.use_minio)

    queries = (
        ["daily_sales", "customer_lifetime_value", "product_performance", "payment_summary", "operations_audit"]
        if args.query == "all"
        else [args.query]
    )

    for q in queries:
        run_query(conn, q)


if __name__ == "__main__":
    main()
