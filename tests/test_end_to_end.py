"""
End-to-End Integration Verification Test
Validates the complete Data Platform flow:
Generation -> Validation -> Storage (Parquet) -> DuckDB Analytics
"""

import os
import shutil
import pytest
import duckdb
import pandas as pd
from data_quality.validation import validate_order_record, validate_cdc_event


@pytest.fixture
def test_lake_dir(tmp_path):
    """Creates a temporary isolated data lake directory for test execution."""
    lake = tmp_path / "test_lake"
    lake.mkdir()
    (lake / "silver" / "orders").mkdir(parents=True)
    (lake / "errors" / "orders").mkdir(parents=True)
    return str(lake)


def test_pipeline_validation_and_dlq_split(test_lake_dir):
    """Verifies that good events reach Silver and bad events reach DLQ."""
    raw_events = [
        {"order_id": 901, "customer_id": 1, "order_status": "PLACED", "total_amount": 1200.0},
        {"order_id": 902, "customer_id": None, "order_status": "PLACED", "total_amount": 800.0},  # Invalid
        {"order_id": 903, "customer_id": 2, "order_status": "DELIVERED", "total_amount": -50.0},  # Invalid
        {"order_id": 904, "customer_id": 3, "order_status": "SHIPPED", "total_amount": 3400.0},
    ]

    silver_records = []
    dlq_records = []

    for ev in raw_events:
        is_valid, reason = validate_order_record(ev)
        enriched = dict(ev)
        enriched["validation_status"] = "VALID" if is_valid else "INVALID"
        enriched["validation_reason"] = reason
        if is_valid:
            silver_records.append(enriched)
        else:
            dlq_records.append(enriched)

    assert len(silver_records) == 2
    assert len(dlq_records) == 2

    # Save to Parquet
    df_silver = pd.DataFrame(silver_records)
    df_dlq = pd.DataFrame(dlq_records)

    silver_path = os.path.join(test_lake_dir, "silver", "orders", "orders.parquet")
    dlq_path = os.path.join(test_lake_dir, "errors", "orders", "dlq.parquet")

    df_silver.to_parquet(silver_path, index=False)
    df_dlq.to_parquet(dlq_path, index=False)

    assert os.path.exists(silver_path)
    assert os.path.exists(dlq_path)

    # Query with DuckDB
    con = duckdb.connect()
    res = con.execute(f"SELECT COUNT(*), SUM(total_amount) FROM read_parquet('{silver_path}')").fetchone()
    assert res[0] == 2
    assert res[1] == 4600.0

    dlq_res = con.execute(f"SELECT COUNT(*) FROM read_parquet('{dlq_path}') WHERE validation_status = 'INVALID'").fetchone()
    assert dlq_res[0] == 2
