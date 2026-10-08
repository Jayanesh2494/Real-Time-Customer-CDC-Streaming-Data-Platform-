# Performance Benchmarking Report

> Real empirical measurements captured locally for the Real-Time CDC & Streaming Platform.

---

## Experiment 1: CSV vs Parquet File Format
- **Dataset Size:** 100,000 transaction records
- **CSV Disk Footprint:** 4.69 MB
- **Parquet Disk Footprint (Snappy compressed):** 2.06 MB
- **Storage Compression Ratio:** **2.28x storage reduction**
- **CSV Full Table Scan & Aggregation:** 106.35 ms
- **Parquet Analytical Scan & Aggregation:** 5.91 ms
- **Scan Acceleration:** **17.99x faster analytical execution**

### Interview Takeaway:
*Parquet stores data column-by-column rather than row-by-row. When executing `SUM(total_amount) GROUP BY order_status`, the query engine only reads the 2 required columns, skipping unwanted attributes completely (projection pushdown). Snappy dictionary encoding further reduces disk I/O.*

---

## Experiment 2: Unpartitioned vs Partitioned Scans
- **Dataset Size:** 100,000 orders across 10 distinct dates
- **Unpartitioned Table Scan:** 14.97 ms
- **Partition Pruned Scan (`order_date='2026-01-01'`):** 10.87 ms
- **Partitioning Speedup:** **1.38x faster**

### Interview Takeaway:
*Partition pruning prevents full-lake directory traversal. By storing data under `order_date=YYYY-MM-DD/`, analytical engines like DuckDB and Spark completely bypass directories outside the filter scope, cutting read I/O proportionally to the number of partitions.*

---

## Experiment 3: Broadcast Join vs Standard Shuffle Join
- **Fact Table:** 100,000 orders
- **Dimension Table:** 500 customers
- **Join Execution Time:** 9.23 ms

### Interview Takeaway:
*In distributed Spark processing, joining a large fact table (Orders) with a small dimension table (Customers) normally triggers an expensive network shuffle of all records across worker nodes. By broadcasting the dimension table (`broadcast(customers)`), each worker retains an in-memory copy of the lookup table, completely eliminating network shuffle and reducing job latency.*
