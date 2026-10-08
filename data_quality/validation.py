"""
Data Quality Validation Engine
Applies quality rules, enriches records with validation metadata, and flags DLQ candidates.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Tuple
import uuid

from .rules import (
    ALLOWED_CDC_OPERATIONS,
    ALLOWED_ORDER_STATUSES,
    ALLOWED_PAYMENT_METHODS,
    ALLOWED_PAYMENT_STATUSES,
    is_non_negative_number,
    is_valid_email,
)


def validate_customer_record(record: Dict[str, Any]) -> Tuple[bool, str | None]:
    """Validate customer data fields."""
    if not record.get("customer_id"):
        return False, "customer_id is NULL or missing"
    
    email = record.get("email")
    if not email:
        return False, "email is missing"
    if not is_valid_email(email):
        return False, f"invalid email format: '{email}'"
        
    first_name = record.get("first_name")
    if not first_name or not str(first_name).strip():
        return False, "first_name cannot be empty"

    return True, None


def validate_product_record(record: Dict[str, Any]) -> Tuple[bool, str | None]:
    """Validate product data fields."""
    if not record.get("product_id"):
        return False, "product_id is NULL or missing"

    price = record.get("price")
    if price is None or not is_non_negative_number(price):
        return False, f"price must be >= 0, found: {price}"

    stock = record.get("stock_quantity")
    if stock is None or not is_non_negative_number(stock):
        return False, f"stock_quantity must be >= 0, found: {stock}"

    name = record.get("product_name")
    if not name or not str(name).strip():
        return False, "product_name cannot be empty"

    return True, None


def validate_order_record(record: Dict[str, Any]) -> Tuple[bool, str | None]:
    """Validate order data fields."""
    if not record.get("order_id"):
        return False, "order_id is NULL or missing"

    if not record.get("customer_id"):
        return False, "customer_id is NULL or missing"

    status = record.get("order_status")
    if not status or str(status).upper() not in ALLOWED_ORDER_STATUSES:
        return False, f"invalid order_status: '{status}'. Allowed: {sorted(list(ALLOWED_ORDER_STATUSES))}"

    total = record.get("total_amount")
    if total is None or not is_non_negative_number(total):
        return False, f"total_amount must be >= 0, found: {total}"

    return True, None


def validate_payment_record(record: Dict[str, Any]) -> Tuple[bool, str | None]:
    """Validate payment data fields."""
    if not record.get("payment_id"):
        return False, "payment_id is NULL or missing"

    if not record.get("order_id"):
        return False, "order_id is NULL or missing"

    amount = record.get("payment_amount")
    if amount is None or not is_non_negative_number(amount):
        return False, f"payment_amount must be >= 0, found: {amount}"

    status = record.get("payment_status")
    if not status or str(status).upper() not in ALLOWED_PAYMENT_STATUSES:
        return False, f"invalid payment_status: '{status}'. Allowed: {sorted(list(ALLOWED_PAYMENT_STATUSES))}"

    method = record.get("payment_method")
    if method and str(method).upper() not in ALLOWED_PAYMENT_METHODS:
        return False, f"invalid payment_method: '{method}'. Allowed: {sorted(list(ALLOWED_PAYMENT_METHODS))}"

    return True, None


def validate_cdc_event(event: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """
    Validate a Debezium CDC envelope and enrich with metadata.
    Envelopes contain 'op' (operation: c/u/d/r), 'before', 'after', 'ts_ms', 'source'.
    """
    op = event.get("op") or event.get("operation")
    if not op or str(op).lower() not in ALLOWED_CDC_OPERATIONS:
        return {
            "validation_status": "INVALID",
            "validation_reason": f"invalid or missing CDC operation: '{op}'",
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_run_id": str(uuid.uuid4()),
            "raw_payload": event,
        }

    op = str(op).lower()
    # For deletes, evaluate 'before' image; for inserts/updates/reads, evaluate 'after' image
    payload = event.get("before") if op == "d" else event.get("after")

    if payload is None:
        return {
            "validation_status": "INVALID",
            "validation_reason": f"payload for operation '{op}' is NULL",
            "processing_timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_run_id": str(uuid.uuid4()),
            "raw_payload": event,
        }

    # Dispatch to specific table validator
    validators = {
        "customers": validate_customer_record,
        "products": validate_product_record,
        "orders": validate_order_record,
        "payments": validate_payment_record,
    }

    validator = validators.get(table_name)
    if validator:
        is_valid, reason = validator(payload)
    else:
        is_valid, reason = True, None

    result = dict(payload)
    result["operation"] = op
    result["source_table"] = table_name
    result["event_timestamp"] = event.get("ts_ms") or int(datetime.now(timezone.utc).timestamp() * 1000)
    result["processing_timestamp"] = datetime.now(timezone.utc).isoformat()
    result["pipeline_run_id"] = str(uuid.uuid4())
    result["validation_status"] = "VALID" if is_valid else "INVALID"
    result["validation_reason"] = reason

    return result
