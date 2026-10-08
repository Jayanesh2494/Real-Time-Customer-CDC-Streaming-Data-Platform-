"""
Data Quality Framework for E-Commerce Streaming and Batch Pipelines.
"""

from .rules import (
    ALLOWED_ORDER_STATUSES,
    ALLOWED_PAYMENT_STATUSES,
    ALLOWED_CDC_OPERATIONS,
    EMAIL_REGEX_PATTERN,
)
from .validation import (
    validate_customer_record,
    validate_product_record,
    validate_order_record,
    validate_payment_record,
    validate_cdc_event,
)

__all__ = [
    "ALLOWED_ORDER_STATUSES",
    "ALLOWED_PAYMENT_STATUSES",
    "ALLOWED_CDC_OPERATIONS",
    "EMAIL_REGEX_PATTERN",
    "validate_customer_record",
    "validate_product_record",
    "validate_order_record",
    "validate_payment_record",
    "validate_cdc_event",
]
