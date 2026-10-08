# Operational Troubleshooting Guide

---

## 1. PostgreSQL & Logical Replication Diagnostics

### Symptom: Debezium connector fails or no WAL events are captured
1. **Check WAL level:**
   ```bash
   docker compose exec postgres psql -U admin -d ecommerce -c "SHOW wal_level;"
   ```
   *Expected output:* `logical`. If it shows `replica` or `minimal`, check `docker-compose.yml` command args.

2. **Inspect Replication Slots:**
   ```bash
   docker compose exec postgres psql -U admin -d ecommerce -c "SELECT slot_name, plugin, active, restart_lsn FROM pg_replication_slots;"
   ```
   *Fix:* If a slot is inactive or abandoned, drop it:
   ```sql
   SELECT pg_drop_replication_slot('debezium');
   ```

3. **Check Table Replica Identity:**
   ```bash
   docker compose exec postgres psql -U admin -d ecommerce -c "\d+ orders"
   ```
   Verify `Replica Identity: FULL`. If `DEFAULT`, run:
   ```sql
   ALTER TABLE orders REPLICA IDENTITY FULL;
   ```

---

## 2. Debezium Connect Diagnostics

### Symptom: Connector state is FAILED or tasks are crashed
1. **Check Connector Status via REST API:**
   ```bash
   curl -s http://localhost:8083/connectors/ecommerce-postgres-connector/status | jq .
   ```

2. **Inspect Debezium Container Logs:**
   ```bash
   docker compose logs --tail=100 debezium-connect
   ```

3. **Restart Failed Connector Task:**
   ```bash
   curl -X POST http://localhost:8083/connectors/ecommerce-postgres-connector/tasks/0/restart
   ```

4. **Re-register Connector:**
   ```bash
   python cdc/debezium/register_connector.py
   ```

---

## 3. Apache Kafka Diagnostics

### Symptom: Topic missing or consumer not receiving messages
1. **List all active topics:**
   ```bash
   docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
   ```

2. **Describe topic details and partition offsets:**
   ```bash
   docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic ecommerce.public.orders
   ```

3. **Inspect Consumer Group Lag:**
   ```bash
   docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group spark-cdc-orders
   ```

4. **Tail Live Kafka Messages from Console:**
   ```bash
   docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic ecommerce.public.orders --from-beginning --max-messages 5
   ```

---

## 4. Apache Spark Streaming Diagnostics

### Symptom: Stream fails or terminates unexpectedly
1. **Check Spark Master & Worker Logs:**
   ```bash
   docker compose logs --tail=100 spark-master
   docker compose logs --tail=100 spark-worker
   ```

2. **Corrupted Checkpoint Recovery:**
   If a checkpoint becomes corrupted during abrupt shutdowns, delete the checkpoint directory in MinIO or the volume:
   ```bash
   docker compose exec minio mc rm --recursive --force myminio/ecommerce-data/checkpoints/orders_silver
   ```

3. **S3A / MinIO Connectivity Check:**
   Ensure `spark.hadoop.fs.s3a.endpoint` is reachable from inside the container (`http://minio:9000`), `path.style.access` is `true`, and `ssl.enabled` is `false`.

---

## 5. MinIO Data Lake Diagnostics

### Symptom: Access denied or bucket not found
1. **Inspect Buckets via MinIO Client (`mc`):**
   ```bash
   docker compose exec minio mc ls myminio/ecommerce-data/
   ```

2. **Check Parquet files inside Silver layer:**
   ```bash
   docker compose exec minio mc ls myminio/ecommerce-data/silver/orders/
   ```

3. **Web Console Access:**
   Open browser at `http://localhost:9001` (User: `admin`, Password: `password123`).

---

## 6. DuckDB Query Diagnostics

### Symptom: Parquet file not found or schema mismatch
1. **Verify local paths or MinIO HTTPFS credentials:**
   If querying local files, verify `data/silver/orders/` contains `.parquet` files.
2. **Inspect Parquet Metadata:**
   ```python
   import duckdb
   con = duckdb.connect()
   print(con.execute("SELECT * FROM parquet_schema('data/batch/orders.parquet')").fetchall())
   ```
