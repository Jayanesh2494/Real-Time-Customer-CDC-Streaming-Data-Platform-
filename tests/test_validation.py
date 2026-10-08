"""
Unit Tests for Data Quality Validation Framework
Tests customer, product, order, payment validation, and Debezium CDC envelope handling.
"""

import pytest
from data_quality.validation import (
    validate_customer_record,
    validate_product_record,
    validate_order_record,
    validate_payment_record,
    validate_cdc_event,
)


def test_valid_customer():
    record = {
        "customer_id": 101,
        "first_name": "Rohan",
        "last_name": "Verma",
        "email": "rohan.verma@example.com",
        "city": "Bengaluru",
        "state": "Karnataka",
    }
    is_valid, reason = validate_customer_record(record)
    assert is_valid is True
    assert reason is None


def test_invalid_customer_null_id():
    record = {
        "customer_id": None,
        "first_name": "Rohan",
        "last_name": "Verma",
        "email": "rohan@example.com",
    }
    is_valid, reason = validate_customer_record(record)
    assert is_valid is False
    assert "customer_id is NULL" in reason


def test_invalid_customer_bad_email():
    record = {
        "customer_id": 101,
        "first_name": "Rohan",
        "last_name": "Verma",
        "email": "not-an-email-address",
    }
    is_valid, reason = validate_customer_record(record)
    assert is_valid is False
    assert "invalid email format" in reason


def test_valid_order():
    record = {
        "order_id": 5001,
        "customer_id": 101,
        "order_status": "PLACED",
        "total_amount": 2499.50,
    }
    is_valid, reason = validate_order_record(record)
    assert is_valid is True
    assert reason is None


def test_invalid_order_negative_amount():
    record = {
        "order_id": 5002,
        "customer_id": 101,
        "order_status": "PLACED",
        "total_amount": -150.00,
    }
    is_valid, reason = validate_order_record(record)
    assert is_valid is False
    assert "total_amount must be >= 0" in reason


def test_invalid_order_status():
    record = {
        "order_id": 5003,
        "customer_id": 101,
        "order_status": "UNKNOWN_STATUS",
        "total_amount": 500.00,
    }
    is_valid, reason = validate_order_record(record)
    assert is_valid is False
    assert "invalid order_status" in reason


def test_valid_product():
    record = {
        "product_id": 201,
        "product_name": "Mechanical Keyboard",
        "category": "Accessories",
        "price": 4999.00,
        "stock_quantity": 50,
    }
    is_valid, reason = validate_product_record(record)
    assert is_valid is True
    assert reason is None


def test_invalid_product_negative_price():
    record = {
        "product_id": 202,
        "product_name": "Broken Item",
        "category": "Accessories",
        "price": -10.00,
        "stock_quantity": 5,
    }
    is_valid, reason = validate_product_record(record)
    assert is_valid is False
    assert "price must be >= 0" in reason


def test_valid_payment():
    record = {
        "payment_id": 801,
        "order_id": 5001,
        "payment_method": "UPI",
        "payment_status": "SUCCESS",
        "payment_amount": 2499.50,
    }
    is_valid, reason = validate_payment_record(record)
    assert is_valid is True
    assert reason is None


def test_invalid_payment_status():
    record = {
        "payment_id": 802,
        "order_id": 5001,
        "payment_method": "UPI",
        "payment_status": "INVALID_STATE",
        "payment_amount": 500.00,
    }
    is_valid, reason = validate_payment_record(record)
    assert is_valid is False
    assert "invalid payment_status" in reason


def test_cdc_event_envelope_validation():
    event = {
        "op": "c",
        "ts_ms": 1700000000000,
        "before": None,
        "after": {
            "order_id": 9001,
            "customer_id": 105,
            "order_status": "CONFIRMED",
            "total_amount": 1200.00,
        },
    }
    enriched = validate_cdc_event(event, table_name="orders")
    assert enriched["validation_status"] == "VALID"
    assert enriched["operation"] == "c"
    assert enriched["order_id"] == 9001
    assert "pipeline_run_id" in enriched
