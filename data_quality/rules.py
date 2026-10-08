"""
Data Quality Rules Definitions
Contains domain constants, regular expressions, and constraint thresholds.
"""

import re

# Allowed Enum values
ALLOWED_ORDER_STATUSES = {
    "PLACED",
    "CONFIRMED",
    "PROCESSING",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "REFUNDED",
}

ALLOWED_PAYMENT_STATUSES = {
    "SUCCESS",
    "PENDING",
    "FAILED",
    "REFUNDED",
}

ALLOWED_PAYMENT_METHODS = {
    "CREDIT_CARD",
    "DEBIT_CARD",
    "UPI",
    "NET_BANKING",
    "WALLET",
    "COD",
}

ALLOWED_CDC_OPERATIONS = {
    "c",  # create (INSERT)
    "u",  # update (UPDATE)
    "d",  # delete (DELETE)
    "r",  # read (initial snapshot)
}

# Standard RFC 5322 compliant regex for email validation
EMAIL_REGEX_PATTERN = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
_COMPILED_EMAIL_REGEX = re.compile(EMAIL_REGEX_PATTERN)


def is_valid_email(email: str | None) -> bool:
    """Validate email address format."""
    if not email or not isinstance(email, str):
        return False
    return bool(_COMPILED_EMAIL_REGEX.match(email.strip()))


def is_non_negative_number(val: int | float | None) -> bool:
    """Check if value is a non-negative numeric quantity."""
    if val is None:
        return False
    try:
        return float(val) >= 0.0
    except (ValueError, TypeError):
        return False
