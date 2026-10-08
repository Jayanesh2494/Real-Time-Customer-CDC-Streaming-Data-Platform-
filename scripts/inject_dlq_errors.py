"""
Dead-Letter Queue (DLQ) Error Injector
Simulates poison pill transactions and malformed records to demonstrate
the Dead-Letter Queue routing and incident audit triage in DuckDB and Spark.
"""

import os
import sys
import uuid
from datetime import datetime, timezone
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_quality.validation import (
    validate_customer_record,
    validate_order_record,
    validate_payment_record,
    validate_product_record,
)


def generate_poison_records(count=20):
    """Generates synthetic poison records that intentionally violate quality rules."""
    records = []

    # 1. Negative total amount orders
    for i in range(5):
        ord_id = 9000 + i
        rec = {
            "order_id": ord_id,
            "customer_id": 10 + i,
            "order_status": "PLACED",
            "total_amount": -250.00 * (i + 1),  # VIOLATION: Negative amount
            "order_date": datetime.now(timezone.utc).isoformat(),
        }
        is_valid, reason = validate_order_record(rec)
        records.append({
            "source_table": "orders",
            "record_id": ord_id,
            "validation_status": "INVALID",
            "validation_reason": reason,
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_run_id": str(uuid.uuid4()),
            "raw_payload": str(rec),
        })

    # 2. Invalid status enum orders
    for i in range(4):
        ord_id = 9100 + i
        rec = {
            "order_id": ord_id,
            "customer_id": 20 + i,
            "order_status": "CORRUPTED_STATUS",  # VIOLATION: Illegal enum
            "total_amount": 1500.00,
            "order_date": datetime.now(timezone.utc).isoformat(),
        }
        is_valid, reason = validate_order_record(rec)
        records.append({
            "source_table": "orders",
            "record_id": ord_id,
            "validation_status": "INVALID",
            "validation_reason": reason,
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_run_id": str(uuid.uuid4()),
            "raw_payload": str(rec),
        })

    # 3. Invalid customer emails
    for i in range(4):
        cid = 9200 + i
        rec = {
            "customer_id": cid,
            "first_name": "BadEmailUser",
            "last_name": "Tester",
            "email": "not-an-email-format-missing-at-domain",  # VIOLATION: Bad regex
            "city": "Bengaluru",
            "state": "Karnataka",
        }
        is_valid, reason = validate_customer_record(rec)
        records.append({
            "source_table": "customers",
            "record_id": cid,
            "validation_status": "INVALID",
            "validation_reason": reason,
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_run_id": str(uuid.uuid4()),
            "raw_payload": str(rec),
        })

    # 4. Negative payments
    for i in range(4):
        pid = 9300 + i
        rec = {
            "payment_id": pid,
            "order_id": 1000 + i,
            "payment_method": "UPI",
            "payment_status": "SUCCESS",
            "payment_amount": -500.00,  # VIOLATION: Negative payment
            "payment_date": datetime.now(timezone.utc).isoformat(),
        }
        is_valid, reason = validate_payment_record(rec)
        records.append({
            "source_table": "payments",
            "record_id": pid,
            "validation_status": "INVALID",
            "validation_reason": reason,
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_run_id": str(uuid.uuid4()),
            "raw_payload": str(rec),
        })

    # 5. Null primary keys
    for i in range(3):
        rec = {
            "customer_id": None,  # VIOLATION: Null PK
            "first_name": "NullUser",
            "last_name": "Tester",
            "email": "nulluser@example.com",
        }
        is_valid, reason = validate_customer_record(rec)
        records.append({
            "source_table": "customers",
            "record_id": 0,
            "validation_status": "INVALID",
            "validation_reason": reason,
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_run_id": str(uuid.uuid4()),
            "raw_payload": str(rec),
        })

    return records


def main():
    errors_dir = os.path.join("data", "errors")
    os.makedirs(errors_dir, exist_ok=True)

    poison_records = generate_poison_records()
    df = pd.DataFrame(poison_records)

    # Save to data/errors/
    out_file = os.path.join(errors_dir, "dlq_incidents.parquet")
    df.to_parquet(out_file, index=False)
    print(f"Successfully generated {len(df)} poison records and routed to DLQ -> {out_file}")


if __name__ == "__main__":
    main()
