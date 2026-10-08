# Data Engineering Interview Guide: Real-Time CDC & Streaming Platform

Comprehensive defense guide covering the 10 architecture questions and 20 advanced system design scenarios expected in Enterprise Data Engineer & Streaming Data Analyst technical interviews.

---

## Part 1: Core Architecture Defense (Questions 1 - 10)

### Q1. Why use Kafka instead of writing directly from Debezium/Postgres to Spark or S3?
**Answer:** Kafka acts as a durable, decoupled message buffer. If Spark or MinIO experiences temporary downtime or maintenance, Kafka retains the CDC events safely without back-pressuring or impacting the transactional database. Kafka also allows multiple independent consumers (real-time Spark streaming, fraud detection models, auditing logs, real-time dashboards) to read from the exact same event stream at their own pace without redundant database load.

### Q2. Why use CDC over periodic batch extraction (SELECT * FROM table)?
**Answer:** Full database polling places heavy query load and locks on operational transactional tables, causes high network transfer overhead, and cannot capture interim changes (e.g., if a record is updated 3 times between batch runs, batch only sees the final state). Furthermore, hard DELETEs cannot be detected by batch polling without complex soft-delete triggers. CDC reads the PostgreSQL Write-Ahead Log (WAL) with sub-second latency, zero query locking, and captures every INSERT, UPDATE, and DELETE event faithfully.

### Q3. Why Apache Spark for stream processing?
**Answer:** Apache Spark Structured Streaming provides distributed, fault-tolerant processing with exact-once micro-batch semantics, seamless unified APIs for both streaming and batch, stateful watermarking, and native integration with columnar formats like Parquet and Delta Lake. It scales horizontally across compute clusters when event volume increases.

### Q4. Why Parquet instead of CSV or JSON for the analytical data lake?
**Answer:** Parquet is a columnar binary storage format that provides:
1. **Projection Pushdown:** Only scans the specific columns referenced in a query, drastically reducing disk I/O.
2. **Predicate Pushdown:** Uses file metadata, statistics (min/max), and dictionary filtering to skip irrelevant row groups entirely.
3. **High Compression:** Achieves 4x to 8x compression using dictionary encoding and Snappy compression compared to raw CSV/JSON.
4. **Strong Typing & Schema Preservation:** Eliminates schema inference overhead and corruption risks.

### Q5. Why MinIO?
**Answer:** MinIO provides an enterprise S3-compliant object storage server that runs 100% locally. It uses the exact same S3 API contracts (`s3a://`), credentials, and bucket structure as AWS S3, enabling realistic cloud data lake simulation (Bronze/Silver/Gold) without incurring cloud provider billing or AWS credentials dependency.

### Q6. Why DuckDB?
**Answer:** DuckDB is an embedded columnar OLAP engine designed for sub-second analytical SQL directly over Parquet files and remote S3/MinIO endpoints (via `httpfs`). It requires zero external server setup, supports vectorized SIMD query execution, and matches or exceeds cloud warehouse performance for single-node data exploration and transformation.

### Q7. How do you handle duplicate events?
**Answer:** Streaming duplicate handling uses a two-pronged strategy:
1. **Deduplication Key:** A deterministic composite key: `(entity_id, operation, event_ts_ms)`.
2. **Stateful Deduplication & Watermarking:** In Spark Structured Streaming, we use `dropDuplicates(["order_id", "operation", "event_ts_ms"])` bounded by a watermark window to prevent unbounded memory growth while filtering duplicate delivery from Kafka.

### Q8. What happens if Spark fails mid-processing?
**Answer:** Spark Structured Streaming maintains persistent state checkpoints on durable object storage (`s3a://ecommerce-data/checkpoints/`). Upon restart, Spark reads the offset commit log, recovers the exact Kafka partition offsets where it left off, and resumes processing without skipping records or producing duplicate output (idempotent write guarantee).

### Q9. What happens if a record is invalid or malformed?
**Answer:** Rather than allowing bad records to crash the streaming pipeline, the engine applies data quality gates. Non-conforming records are tagged with `validation_status="INVALID"`, enriched with `validation_reason` and pipeline metadata, and routed to an isolated Dead-Letter Queue (DLQ) Parquet path (`errors/orders`) and a dedicated Kafka DLQ topic (`ecommerce.dlq.errors`) for monitoring, triage, and replay.

### Q10. How would you scale this system for 10x throughput?
**Answer:**
1. **Kafka:** Increase topic partitions (e.g., from 3 to 12 or 24) to increase parallel consumer throughput.
2. **Spark:** Add Spark worker nodes/executors; allocate 1 executor task per Kafka partition.
3. **Partitioning:** Implement logical time-based (`year/month/day`) or entity-based (`status`) partitioning in the data lake to prevent write hotspots.
4. **Postgres:** Scale with read replicas, connection pooling (PgBouncer), and tuned WAL buffers.

---

## Part 2: Advanced Data Engineering Interview Scenarios (Questions 11 - 30)

### Q11. How do you handle out-of-order CDC events?
**Answer:** Out-of-order events occur due to network retransmissions or multi-partition writes. We handle them by:
- Guaranteeing in-order delivery from Kafka by keying each message on its entity primary key (`customer_id`, `order_id`). Kafka guarantees strict total ordering within a single partition.
- In downstream stateful updates, using the CDC transaction timestamp `ts_ms` or PostgreSQL LSN (Log Sequence Number). A state store only applies updates if `new_event.ts_ms >= current_state.ts_ms`.

### Q12. How do you guarantee idempotent processing across the pipeline?
**Answer:** An idempotent operation produces the exact same outcome whether executed once or multiple times. In our pipeline:
1. Deterministic event identifiers prevent duplicated records upon retry.
2. Parquet outputs are written using partitioned overwrite modes in batch, or deterministic micro-batch append IDs in streaming.
3. Primary keys and snapshot tables in DuckDB/Silver are maintained using upsert/merge logic rather than blind appending.

### Q13. How would you handle schema evolution?
**Answer:**
- **Backward/Forward compatibility:** Introduce Confluent Schema Registry with Avro or Protobuf schemas.
- **Debezium handling:** Debezium captures DDL changes via `schema-changes` topic.
- **Parquet/Spark:** Enable `spark.sql.parquet.mergeSchema = true` so newer Parquet files with additional nullable columns can be queried harmoniously alongside historical files without pipeline interruption.

### Q14. What happens if a Kafka consumer crashes?
**Answer:** Kafka's group coordinator detects the missing heartbeat within `session.timeout.ms`, triggers a consumer group rebalance, and reassigns the orphaned partitions to surviving consumers in the group. Processing resumes from the last committed offset.

### Q15. How do you detect and monitor Kafka consumer lag?
**Answer:** Consumer lag is the difference between the partition log end offset (latest written message) and the consumer's current offset. We track it using `kafka-consumer-groups --describe --group <group-id>` or via Prometheus JMX Exporter and Grafana. Spikes in consumer lag signal downstream processing bottlenecks or resource starvation.

### Q16. How do you handle a poison message?
**Answer:** A poison message is a malformed record (e.g., invalid JSON, corrupt serialization) that repeatedly crashes the consumer parser. We use try/catch blocks in the deserializer to catch deserialization errors, wrap the raw byte payload with failure metadata, publish it directly to the DLQ topic, and commit the consumer offset to prevent pipeline blockage.

### Q17. How do you handle data skew in Spark?
**Answer:** Data skew occurs when one partition key has significantly more records than others (e.g. 80% of orders come from one status or customer), causing one executor task to run for hours while others finish in seconds. Solutions:
1. **Salting:** Append random integers `0..N` to the join key to distribute skewed rows across multiple partitions, join, and then strip the salt.
2. **Adaptive Query Execution (AQE):** Set `spark.sql.adaptive.enabled = true` and `spark.sql.adaptive.skewJoin.enabled = true`, which automatically splits skewed partitions into smaller sub-partitions at runtime.

### Q18. When would you use a broadcast join?
**Answer:** When joining a large fact table (e.g. millions of orders) with a small lookup dimension table (e.g. < 50MB customer or category table). By broadcasting the dimension table to all executors (`broadcast(dim_table)`), Spark executes a map-side join and eliminates the expensive network shuffle of the large fact table.

### Q19. What causes Spark shuffle and why is it expensive?
**Answer:** Spark shuffle occurs when an operation requires redistributing data across executors by key (e.g., `groupByKey`, `reduceByKey`, `join`, `repartition`, `distinct`). It is expensive because it involves serializing data, writing to executor local disks, sending gigabytes over the network, and deserializing on receiving nodes, causing severe CPU, disk I/O, and network bottlenecks.

### Q20. How would you optimize a slow Spark job?
**Answer:**
1. Check the Spark Web UI DAG visualization to identify the bottleneck stage and long-running tasks.
2. Optimize partition counts (`spark.sql.shuffle.partitions` tuned to data size, typically 2-3 tasks per CPU core).
3. Eliminate cartesian and wide shuffle joins using broadcast joins where applicable.
4. Cache/persist intermediate DataFrames reused multiple times.
5. Push filters and column selection as early as possible (projection & predicate pushdown).
6. Enable AQE (Adaptive Query Execution).

### Q21. How do you handle late-arriving events in streaming?
**Answer:** In Spark Structured Streaming, we define a watermark: `withWatermark("event_time", "10 minutes")`. Spark retains state for late data up to the watermark threshold. Events arriving within the watermark window update the aggregate; events arriving later than 10 minutes are dropped or routed to a late-arriving audit log.

### Q22. How do you reprocess historical data?
**Answer:** Because Bronze preserves raw CDC envelopes in object storage and Kafka retains events up to retention window, reprocessing is straightforward:
1. Spin up a separate consumer group or batch job pointing to Bronze S3 storage or Kafka offset 0.
2. Apply updated transformation logic and write to a new Silver staging path.
3. Validate data quality and atomically swap the target path or table pointer.

### Q23. How would you backfill one day of data?
**Answer:** Airflow backfilling: execute `airflow dags backfill ecommerce_batch_pipeline -s 2026-10-01 -e 2026-10-02`. The DAG runs with execution date context, processes the specific daily partition, applies idempotent partition overwrites (`replaceWhere` or partitioned folder replacement), leaving all other days untouched.

### Q24. How do you prevent the "small-file problem" in streaming data lakes?
**Answer:** Frequent streaming micro-batches (e.g. every 5s) create thousands of tiny Parquet files (a few KB each), which degrades read performance due to file system metadata overhead. Solutions:
1. Tune micro-batch triggers to larger intervals (e.g., 30-60 seconds).
2. Schedule a periodic compaction job (via Airflow) that reads micro-batch files from the last hour, coalesces them into larger 128MB-256MB Parquet files, and overwrites the partition.

### Q25. How do you monitor pipeline freshness?
**Answer:** Freshness latency is the duration between the source transaction time (`ts_ms`) and the timestamp when the record becomes queryable in the Silver/Gold layer (`processing_timestamp`). We compute `lag = processing_timestamp - ts_ms` and publish this metric to monitoring dashboards; if lag exceeds SLA (e.g., > 30 seconds), an alert is triggered.

### Q26. How do you recover after complete pipeline failure?
**Answer:**
1. Restore infrastructure services (Postgres, Kafka, MinIO).
2. Debezium resumes from the PostgreSQL replication slot without data loss.
3. Spark Structured Streaming recovers from MinIO checkpoints and resumes consumption from the exact uncommitted Kafka offset.
4. Airflow reruns failed DAG tasks using built-in retries.

### Q27. How do you handle source-schema changes (e.g. new column added in PostgreSQL)?
**Answer:**
1. Postgres logical replication automatically propagates the new column in the WAL.
2. Debezium updates its schema history.
3. Spark Structured Streaming configured with schema evolution or flexible JSON parsing extracts the new field without failing on previous records where the field was absent.

### Q28. How do you implement data lineage?
**Answer:** Data lineage tracks data from origin to destination. In our platform, every record is enriched with provenance metadata: `source_table`, `operation`, `event_timestamp`, `processing_timestamp`, and `pipeline_run_id`. For enterprise deployments, we integrate OpenLineage / Marquez to trace table and column-level transformations across Spark and Airflow.

### Q29. How do you separate Bronze, Silver, and Gold tiers?
**Answer:**
- **Bronze:** Raw, immutable, append-only landing zone. Stores full Debezium CDC envelopes for audit and replay.
- **Silver:** Cleaned, typed, deduplicated, and validated data. Conforms to enterprise schema rules and serves as the single source of truth.
- **Gold:** Curated, aggregated business dimensional models (CLV, Daily Sales, Performance) optimized for executive reporting and analytical SQL queries.

### Q30. Why is Medallion Architecture superior to traditional monolithic data lakes?
**Answer:** It decouples raw ingestion from business modeling. If transformation logic or business rules change, you never have to re-extract from the production operational database; you simply replay from the immutable Bronze layer. It also establishes clean governance gates between raw unverified inputs and business-critical analytical reporting.
