# Architecture Specification: Real-Time CDC & Streaming Platform

## 1. Executive Summary
The **Real-Time Customer CDC & Streaming Data Platform** is an enterprise data platform built entirely using open-source technologies running in a local containerized environment. It solves the challenge of near real-time data ingestion, transformation, harmonization, and analytical accessibility without impacting the transactional PostgreSQL source system.

---

## 2. End-to-End Data Pipeline Architecture

```
                               OPERATIONAL LAYER
                         +---------------------------+
                         |        PostgreSQL         |
                         |  (WAL Logical Replication)|
                         +-------------+-------------+
                                       |
                                    CDC (pgoutput)
                                       |
                                       v
                         +---------------------------+
                         |      Debezium Connect     |
                         +-------------+-------------+
                                       |
                                       v
                                STREAMING LAYER
                         +---------------------------+
                         |       Apache Kafka        |
                         |  (KRaft Distributed Logs) |
                         |---------------------------|
                         | - ecommerce.public.cust   |
                         | - ecommerce.public.orders |
                         | - ecommerce.public.pay    |
                         +-------------+-------------+
                                       |
                                       v
                               PROCESSING LAYER
                         +---------------------------+
                         |  Apache Spark Structured  |
                         |        Streaming          |
                         |---------------------------|
                         | - Deduplication (Keys)    |
                         | - Schema Enforcement      |
                         | - Data Quality Gates      |
                         +------+-------------+------+
                                |             |
                 Valid Records  |             | Invalid Records
                                v             v
                   +----------------+    +-------------------+
                   |  MinIO (S3)    |    | Dead-Letter Queue |
                   |  Silver Layer  |    | (errors/ & dlq)   |
                   +-------+--------+    +-------------------+
                           |
                           v
                     +------------+
                     | MinIO (S3) |
                     | Gold Layer |
                     +-----+------+
                           |
                           v
                    ANALYTICS LAYER
             +----------------------------+
             |           DuckDB           |
             | (Columnar / Parquet / S3)  |
             +----------------------------+
                           |
                           v
             +----------------------------+
             |   BI / Analytics Queries   |
             +----------------------------+
```

---

## 3. Component Breakdown

### 3.1 Source System (PostgreSQL 16)
- **Engine:** PostgreSQL 16 Alpine
- **Configuration:** `wal_level = logical`, `max_wal_senders = 10`, `max_replication_slots = 10`
- **Replication Identity:** `REPLICA IDENTITY FULL` on all transactional tables (`customers`, `products`, `orders`, `order_items`, `payments`).
- **Why `REPLICA IDENTITY FULL`?** Default Postgres replication only logs the primary key on UPDATE and DELETE. `FULL` writes the entire pre-modification row to the write-ahead log, ensuring downstream consumers receive complete `before` images for auditing and deduplication.

### 3.2 Change Data Capture (Debezium Connect)
- **Connector:** `io.debezium.connector.postgresql.PostgresConnector`
- **Plugin:** `pgoutput` (native PostgreSQL logical decoding plugin)
- **Semantics:** Reads changes from the Postgres replication slot asynchronously with zero operational query locking on transaction tables.
- **Envelope Format:** Standard Debezium JSON envelope containing:
  - `op`: Operation type (`c` = create, `u` = update, `d` = delete, `r` = read snapshot).
  - `before`: Pre-change row state.
  - `after`: Post-change row state.
  - `ts_ms`: Transaction timestamp in milliseconds.
  - `source`: Metadata (table, lsn, txId).

### 3.3 Event Backbone (Apache Kafka - KRaft Mode)
- **Distribution:** KRaft (Kafka Raft Metadata Mode) eliminates ZooKeeper, removing a critical point of failure and reducing container memory footprint by ~500 MB.
- **Topics:**
  - `ecommerce.public.customers` (Partitions: 3, Retention: 7 days)
  - `ecommerce.public.orders` (Partitions: 3, Retention: 7 days)
  - `ecommerce.public.payments` (Partitions: 3, Retention: 7 days)
  - `ecommerce.dlq.errors` (Partitions: 3, Retention: 14 days)
- **Partitioning Strategy:** Keyed on entity primary key (`customer_id`, `order_id`, `payment_id`) to ensure strict in-order delivery of state mutations per entity.

### 3.4 Distributed Stream Processing (Apache Spark 3.5)
- **Engine:** PySpark Structured Streaming with micro-batch trigger (`5 seconds`).
- **State & Checkpoints:** Managed on object storage (`s3a://ecommerce-data/checkpoints/`) guaranteeing fault tolerance and recovery without duplicate output.
- **Deduplication:** State-based deduplication on `(entity_id, operation, event_ts_ms)` within a configured watermark window.
- **Data Quality Gates:** Real-time routing:
  - Conforming records -> Silver Parquet.
  - Non-conforming records -> Error Parquet & Kafka DLQ topic with failure diagnostics.

### 3.5 Data Lake Storage (MinIO S3-Compatible Storage)
- **Bucket:** `ecommerce-data`
- **Lake Architecture:**
  - `bronze/`: Immutable landing zone for raw CDC envelopes.
  - `silver/`: Cleaned, typed, deduplicated, and partitioned Parquet datasets.
  - `gold/`: Aggregated business dimensional models (CLV, daily sales, product metrics).
  - `errors/`: Dead-Letter Queue storage preserving poison records for audit and replay.

### 3.6 Analytical SQL Engine (DuckDB)
- **Engine:** DuckDB in-memory / embedded OLAP database.
- **Integration:** Leverages `httpfs` extension for direct zero-copy HTTP/S3 querying over MinIO Parquet files with automatic projection pushdown, dictionary decoding, and vectorization.

### 3.7 Orchestration (Apache Airflow)
- **Role:** Scheduled execution of batch ingestion, historical backfills, and continuous data quality scoring across lake tiers.
