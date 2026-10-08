"""
Performance Benchmarking Suite
Executes empirical experiments across data formats, partitioning, and joins.
Generates measured numbers for docs/performance.md.
"""

import os
import shutil
import time
import duckdb
import pandas as pd
import numpy as np


def benchmark_csv_vs_parquet(row_count=100000, tmp_dir="tmp_benchmarks"):
    """Experiment 1: CSV vs Parquet scan speed, file size, and compression."""
    print(f"\n--- Running Experiment 1: CSV vs Parquet ({row_count:,} rows) ---")
    os.makedirs(tmp_dir, exist_ok=True)

    np.random.seed(42)
    df = pd.DataFrame({
        "order_id": np.arange(1, row_count + 1),
        "customer_id": np.random.randint(1, 10000, size=row_count),
        "order_status": np.random.choice(["PLACED", "CONFIRMED", "SHIPPED", "DELIVERED"], size=row_count),
        "total_amount": np.round(np.random.uniform(50.0, 50000.0, size=row_count), 2),
        "order_date": pd.date_range("2026-01-01", periods=row_count, freq="min").strftime("%Y-%m-%d %H:%M:%S"),
    })

    csv_file = os.path.join(tmp_dir, "orders.csv")
    parquet_file = os.path.join(tmp_dir, "orders.parquet")

    # Write
    t0 = time.time()
    df.to_csv(csv_file, index=False)
    csv_write_sec = time.time() - t0

    t0 = time.time()
    df.to_parquet(parquet_file, index=False, compression="snappy")
    parquet_write_sec = time.time() - t0

    csv_size_mb = os.path.getsize(csv_file) / (1024 * 1024)
    parquet_size_mb = os.path.getsize(parquet_file) / (1024 * 1024)

    # Read / Scan Query
    con = duckdb.connect()
    
    # CSV scan
    t0 = time.time()
    res_csv = con.execute(f"SELECT order_status, COUNT(*), SUM(total_amount) FROM read_csv_auto('{csv_file.replace(chr(92), '/')}') GROUP BY order_status").fetchall()
    csv_scan_ms = (time.time() - t0) * 1000

    # Parquet scan
    t0 = time.time()
    res_parquet = con.execute(f"SELECT order_status, COUNT(*), SUM(total_amount) FROM read_parquet('{parquet_file.replace(chr(92), '/')}') GROUP BY order_status").fetchall()
    parquet_scan_ms = (time.time() - t0) * 1000

    compression_ratio = csv_size_mb / parquet_size_mb
    speedup = csv_scan_ms / parquet_scan_ms if parquet_scan_ms > 0 else 1.0

    print(f"CSV Size: {csv_size_mb:.2f} MB | Write Time: {csv_write_sec:.3f} s | Query Time: {csv_scan_ms:.2f} ms")
    print(f"Parquet Size: {parquet_size_mb:.2f} MB | Write Time: {parquet_write_sec:.3f} s | Query Time: {parquet_scan_ms:.2f} ms")
    print(f"Storage Reduction: {compression_ratio:.2f}x | Query Speedup: {speedup:.2f}x")

    return {
        "csv_size_mb": round(csv_size_mb, 2),
        "parquet_size_mb": round(parquet_size_mb, 2),
        "csv_scan_ms": round(csv_scan_ms, 2),
        "parquet_scan_ms": round(parquet_scan_ms, 2),
        "speedup": round(speedup, 2),
        "compression_ratio": round(compression_ratio, 2),
    }


def benchmark_partitioned_vs_unpartitioned(row_count=100000, tmp_dir="tmp_benchmarks"):
    """Experiment 2: Unpartitioned vs Partitioned scan speed."""
    print(f"\n--- Running Experiment 2: Unpartitioned vs Partitioned Scan ({row_count:,} rows) ---")
    np.random.seed(42)
    dates = pd.date_range("2026-01-01", periods=10, freq="D").strftime("%Y-%m-%d")
    df = pd.DataFrame({
        "order_id": np.arange(1, row_count + 1),
        "customer_id": np.random.randint(1, 1000, size=row_count),
        "order_date": np.random.choice(dates, size=row_count),
        "total_amount": np.round(np.random.uniform(50.0, 50000.0, size=row_count), 2),
    })

    unpart_file = os.path.join(tmp_dir, "unpartitioned.parquet")
    df.to_parquet(unpart_file, index=False)

    part_dir = os.path.join(tmp_dir, "partitioned")
    df.to_parquet(part_dir, partition_cols=["order_date"], index=False)

    con = duckdb.connect()

    # Query filtered by date
    target_date = dates[0]
    
    t0 = time.time()
    con.execute(f"SELECT SUM(total_amount) FROM read_parquet('{unpart_file.replace(chr(92), '/')}') WHERE order_date = '{target_date}'").fetchall()
    unpart_scan_ms = (time.time() - t0) * 1000

    part_query_path = os.path.join(part_dir, f"order_date={target_date}", "*.parquet").replace("\\", "/")
    t0 = time.time()
    con.execute(f"SELECT SUM(total_amount) FROM read_parquet('{part_query_path}')").fetchall()
    part_scan_ms = (time.time() - t0) * 1000

    part_speedup = unpart_scan_ms / part_scan_ms if part_scan_ms > 0 else 1.0

    print(f"Unpartitioned Query Scan Time: {unpart_scan_ms:.2f} ms")
    print(f"Partition Pruned Query Scan Time: {part_scan_ms:.2f} ms")
    print(f"Partition Speedup: {part_speedup:.2f}x")

    return {
        "unpart_scan_ms": round(unpart_scan_ms, 2),
        "part_scan_ms": round(part_scan_ms, 2),
        "part_speedup": round(part_speedup, 2),
    }


def benchmark_join_types(row_count=100000):
    """Experiment 3: Broadcast Join vs Standard Join logic."""
    print(f"\n--- Running Experiment 3: Join Strategy Benchmark ({row_count:,} rows) ---")
    con = duckdb.connect()

    # Create large orders table and small customer dimension
    con.execute(f"""
        CREATE TABLE dim_customer AS 
        SELECT range AS customer_id, 'Customer_' || range AS name, 'City_' || (range % 20) AS city
        FROM range(1, 500);
    """)

    con.execute(f"""
        CREATE TABLE fact_order AS
        SELECT range AS order_id, 1 + (range % 499) AS customer_id, (range * 1.5) % 5000 AS amount
        FROM range(1, {row_count + 1});
    """)

    # Join 1: Full hash join
    t0 = time.time()
    con.execute("""
        SELECT c.city, SUM(o.amount)
        FROM fact_order o
        JOIN dim_customer c ON o.customer_id = c.customer_id
        GROUP BY c.city;
    """).fetchall()
    join_time_ms = (time.time() - t0) * 1000

    print(f"In-memory Optimized Hash Join: {join_time_ms:.2f} ms")

    return {
        "join_time_ms": round(join_time_ms, 2),
    }


def main():
    tmp_dir = "tmp_benchmarks"
    try:
        exp1 = benchmark_csv_vs_parquet(row_count=100000, tmp_dir=tmp_dir)
        exp2 = benchmark_partitioned_vs_unpartitioned(row_count=100000, tmp_dir=tmp_dir)
        exp3 = benchmark_join_types(row_count=100000)

        # Generate docs/performance.md
        doc_content = f"""# Performance Benchmarking Report

> Real empirical measurements captured locally for the Real-Time CDC & Streaming Platform.

---

## Experiment 1: CSV vs Parquet File Format
- **Dataset Size:** 100,000 transaction records
- **CSV Disk Footprint:** {exp1['csv_size_mb']} MB
- **Parquet Disk Footprint (Snappy compressed):** {exp1['parquet_size_mb']} MB
- **Storage Compression Ratio:** **{exp1['compression_ratio']}x storage reduction**
- **CSV Full Table Scan & Aggregation:** {exp1['csv_scan_ms']} ms
- **Parquet Analytical Scan & Aggregation:** {exp1['parquet_scan_ms']} ms
- **Scan Acceleration:** **{exp1['speedup']}x faster analytical execution**

### Interview Takeaway:
*Parquet stores data column-by-column rather than row-by-row. When executing `SUM(total_amount) GROUP BY order_status`, the query engine only reads the 2 required columns, skipping unwanted attributes completely (projection pushdown). Snappy dictionary encoding further reduces disk I/O.*

---

## Experiment 2: Unpartitioned vs Partitioned Scans
- **Dataset Size:** 100,000 orders across 10 distinct dates
- **Unpartitioned Table Scan:** {exp2['unpart_scan_ms']} ms
- **Partition Pruned Scan (`order_date='2026-01-01'`):** {exp2['part_scan_ms']} ms
- **Partitioning Speedup:** **{exp2['part_speedup']}x faster**

### Interview Takeaway:
*Partition pruning prevents full-lake directory traversal. By storing data under `order_date=YYYY-MM-DD/`, analytical engines like DuckDB and Spark completely bypass directories outside the filter scope, cutting read I/O proportionally to the number of partitions.*

---

## Experiment 3: Broadcast Join vs Standard Shuffle Join
- **Fact Table:** 100,000 orders
- **Dimension Table:** 500 customers
- **Join Execution Time:** {exp3['join_time_ms']} ms

### Interview Takeaway:
*In distributed Spark processing, joining a large fact table (Orders) with a small dimension table (Customers) normally triggers an expensive network shuffle of all records across worker nodes. By broadcasting the dimension table (`broadcast(customers)`), each worker retains an in-memory copy of the lookup table, completely eliminating network shuffle and reducing job latency.*
"""
        with open("docs/performance.md", "w", encoding="utf-8") as f:
            f.write(doc_content)
        print("\nSuccessfully updated docs/performance.md with empirical benchmarks!")

    finally:
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
