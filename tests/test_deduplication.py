"""
Unit Tests for Deduplication and Idempotency
Verifies that streaming deduplication keys prevent duplicate writes and ensure idempotent retries.
"""

import pytest
import pandas as pd


def deduplicate_events(events: list[dict], key_cols: list[str]) -> list[dict]:
    """Simulates Spark/Streaming deduplication logic using event composite keys."""
    seen_keys = set()
    deduped = []
    for ev in events:
        composite_key = tuple(ev.get(col) for col in key_cols)
        if composite_key not in seen_keys:
            seen_keys.add(composite_key)
            deduped.append(ev)
    return deduped


def test_duplicate_order_events():
    events = [
        {"order_id": 1001, "operation": "c", "event_ts_ms": 1700000000000, "total_amount": 1500.0},
        {"order_id": 1001, "operation": "c", "event_ts_ms": 1700000000000, "total_amount": 1500.0},  # Duplicate
        {"order_id": 1001, "operation": "c", "event_ts_ms": 1700000000000, "total_amount": 1500.0},  # Duplicate
        {"order_id": 1002, "operation": "c", "event_ts_ms": 1700000000100, "total_amount": 2500.0},
    ]

    deduped = deduplicate_events(events, ["order_id", "operation", "event_ts_ms"])
    assert len(deduped) == 2
    assert [d["order_id"] for d in deduped] == [1001, 1002]


def test_idempotent_reprocessing():
    """Verifies that re-running the same batch yields identical output without duplication."""
    batch_initial = [
        {"order_id": 2001, "operation": "c", "event_ts_ms": 1700000001000},
        {"order_id": 2002, "operation": "c", "event_ts_ms": 1700000002000},
    ]

    batch_retry = [
        {"order_id": 2001, "operation": "c", "event_ts_ms": 1700000001000},
        {"order_id": 2002, "operation": "c", "event_ts_ms": 1700000002000},
        {"order_id": 2003, "operation": "c", "event_ts_ms": 1700000003000},
    ]

    initial_run = deduplicate_events(batch_initial, ["order_id", "operation", "event_ts_ms"])
    combined = deduplicate_events(initial_run + batch_retry, ["order_id", "operation", "event_ts_ms"])

    assert len(combined) == 3
    assert set(d["order_id"] for d in combined) == {2001, 2002, 2003}


def test_state_updates_preserved():
    """Different operations on same entity must NOT be dropped."""
    events = [
        {"order_id": 3001, "operation": "c", "event_ts_ms": 1700000010000, "order_status": "PLACED"},
        {"order_id": 3001, "operation": "u", "event_ts_ms": 1700000020000, "order_status": "SHIPPED"},
        {"order_id": 3001, "operation": "u", "event_ts_ms": 1700000030000, "order_status": "DELIVERED"},
    ]

    deduped = deduplicate_events(events, ["order_id", "operation", "event_ts_ms"])
    assert len(deduped) == 3
