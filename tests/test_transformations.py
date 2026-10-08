"""
Unit Tests for Silver-to-Gold Analytical Transformations
Tests business aggregation logic for sales, customer lifetime value, and payment metrics.
"""

import pytest
import pandas as pd


def test_daily_sales_calculation():
    orders = pd.DataFrame([
        {"order_id": 1, "order_status": "DELIVERED", "total_amount": 1000.0, "order_date": "2026-10-01"},
        {"order_id": 2, "order_status": "SHIPPED", "total_amount": 2500.0, "order_date": "2026-10-01"},
        {"order_id": 3, "order_status": "CANCELLED", "total_amount": 500.0, "order_date": "2026-10-01"},
        {"order_id": 4, "order_status": "DELIVERED", "total_amount": 1500.0, "order_date": "2026-10-02"},
    ])

    active_orders = orders[orders["order_status"] != "CANCELLED"]
    daily_revenue = active_orders.groupby("order_date")["total_amount"].sum().to_dict()

    assert daily_revenue["2026-10-01"] == 3500.0
    assert daily_revenue["2026-10-02"] == 1500.0


def test_customer_lifetime_value_calculation():
    customers = pd.DataFrame([
        {"customer_id": 10, "first_name": "Aarav"},
        {"customer_id": 20, "first_name": "Diya"},
    ])

    orders = pd.DataFrame([
        {"order_id": 1, "customer_id": 10, "total_amount": 1200.0, "order_status": "DELIVERED"},
        {"order_id": 2, "customer_id": 10, "total_amount": 800.0, "order_status": "DELIVERED"},
        {"order_id": 3, "customer_id": 20, "total_amount": 5000.0, "order_status": "DELIVERED"},
    ])

    clv = orders.groupby("customer_id").agg(
        lifetime_orders=("order_id", "count"),
        lifetime_value=("total_amount", "sum"),
        avg_basket=("total_amount", "mean"),
    ).reset_index()

    merged = customers.merge(clv, on="customer_id")
    cust_10 = merged[merged["customer_id"] == 10].iloc[0]
    cust_20 = merged[merged["customer_id"] == 20].iloc[0]

    assert cust_10["lifetime_orders"] == 2
    assert cust_10["lifetime_value"] == 2000.0
    assert cust_10["avg_basket"] == 1000.0
    assert cust_20["lifetime_value"] == 5000.0


def test_payment_success_rate():
    payments = pd.DataFrame([
        {"payment_id": 1, "payment_method": "UPI", "payment_status": "SUCCESS"},
        {"payment_id": 2, "payment_method": "UPI", "payment_status": "SUCCESS"},
        {"payment_id": 3, "payment_method": "UPI", "payment_status": "FAILED"},
        {"payment_id": 4, "payment_method": "CREDIT_CARD", "payment_status": "SUCCESS"},
    ])

    upi_payments = payments[payments["payment_method"] == "UPI"]
    success_count = (upi_payments["payment_status"] == "SUCCESS").sum()
    total_count = len(upi_payments)
    success_rate = (success_count / total_count) * 100

    assert round(success_rate, 2) == 66.67
