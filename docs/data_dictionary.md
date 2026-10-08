# E-Commerce Data Platform Data Dictionary

---

## 1. Operational & Silver Entities

### 1.1 customers
Represents registered buyers in the e-commerce application.

| Column Name | SQL Type | Parquet Type | Nullable | Description & Constraints |
| :--- | :--- | :--- | :--- | :--- |
| `customer_id` | `BIGINT` | `INT64` | No | Primary Key. Unique customer identifier. |
| `first_name` | `VARCHAR(100)` | `UTF8` | No | Customer's first name. Non-empty. |
| `last_name` | `VARCHAR(100)` | `UTF8` | No | Customer's last name. Non-empty. |
| `email` | `VARCHAR(255)` | `UTF8` | No | Customer's email. Must adhere to RFC 5322 regex. Unique. |
| `city` | `VARCHAR(100)` | `UTF8` | No | Primary residential/shipping city. |
| `state` | `VARCHAR(100)` | `UTF8` | No | State or province. Partition column in Silver lake. |
| `created_at` | `TIMESTAMP` | `TIMESTAMP_MICROS` | No | Account registration timestamp. |
| `updated_at` | `TIMESTAMP` | `TIMESTAMP_MICROS` | No | Last account modification timestamp. |

---

### 1.2 products
Represents items available for purchase in catalog.

| Column Name | SQL Type | Parquet Type | Nullable | Description & Constraints |
| :--- | :--- | :--- | :--- | :--- |
| `product_id` | `BIGINT` | `INT64` | No | Primary Key. Unique product identifier. |
| `product_name` | `VARCHAR(255)` | `UTF8` | No | Commercial name of item. Non-empty. |
| `category` | `VARCHAR(100)` | `UTF8` | No | Catalog classification (Electronics, Apparel, etc.). Partition column. |
| `price` | `NUMERIC(12,2)` | `DOUBLE` | No | Unit price in INR. Must be `>= 0.00`. |
| `stock_quantity` | `INTEGER` | `INT32` | No | Available warehouse inventory. Must be `>= 0`. |
| `created_at` | `TIMESTAMP` | `TIMESTAMP_MICROS` | No | Creation timestamp in catalog. |
| `updated_at` | `TIMESTAMP` | `TIMESTAMP_MICROS` | No | Last catalog update timestamp. |

---

### 1.3 orders
Represents purchase transactions placed by customers.

| Column Name | SQL Type | Parquet Type | Nullable | Description & Constraints |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | `BIGINT` | `INT64` | No | Primary Key. Unique transaction identifier. |
| `customer_id` | `BIGINT` | `INT64` | No | Foreign Key referencing `customers.customer_id`. |
| `order_status` | `VARCHAR(50)` | `UTF8` | No | State: `PLACED`, `CONFIRMED`, `PROCESSING`, `SHIPPED`, `DELIVERED`, `CANCELLED`, `REFUNDED`. Partition column. |
| `total_amount` | `NUMERIC(12,2)` | `DOUBLE` | No | Aggregate transaction value. Must be `>= 0.00`. |
| `order_date` | `TIMESTAMP` | `TIMESTAMP_MICROS` | No | Placement timestamp. |
| `updated_at` | `TIMESTAMP` | `TIMESTAMP_MICROS` | No | Order status transition timestamp. |

---

### 1.4 order_items
Represents line items composing an order.

| Column Name | SQL Type | Parquet Type | Nullable | Description & Constraints |
| :--- | :--- | :--- | :--- | :--- |
| `order_item_id` | `BIGINT` | `INT64` | No | Primary Key. Line item identifier. |
| `order_id` | `BIGINT` | `INT64` | No | Foreign Key referencing `orders.order_id`. |
| `product_id` | `BIGINT` | `INT64` | No | Foreign Key referencing `products.product_id`. |
| `quantity` | `INTEGER` | `INT32` | No | Purchased quantity. Must be `> 0`. |
| `unit_price` | `NUMERIC(12,2)` | `DOUBLE` | No | Unit price at time of order. Must be `>= 0.00`. |

---

### 1.5 payments
Represents financial settlements associated with orders.

| Column Name | SQL Type | Parquet Type | Nullable | Description & Constraints |
| :--- | :--- | :--- | :--- | :--- |
| `payment_id` | `BIGINT` | `INT64` | No | Primary Key. Unique settlement identifier. |
| `order_id` | `BIGINT` | `INT64` | No | Foreign Key referencing `orders.order_id`. |
| `payment_method` | `VARCHAR(50)` | `UTF8` | No | Method: `CREDIT_CARD`, `DEBIT_CARD`, `UPI`, `NET_BANKING`, `WALLET`, `COD`. Partition column. |
| `payment_status` | `VARCHAR(50)` | `UTF8` | No | Status: `SUCCESS`, `PENDING`, `FAILED`, `REFUNDED`. |
| `payment_amount` | `NUMERIC(12,2)` | `DOUBLE` | No | Settlement amount. Must be `>= 0.00`. |
| `payment_date` | `TIMESTAMP` | `TIMESTAMP_MICROS` | No | Payment settlement timestamp. |

---

## 2. Dead-Letter Queue (DLQ) Schema (`errors/*`)
Records that violate schema contracts or data quality rules are enriched with diagnostics and routed here.

| Column Name | Parquet Type | Description |
| :--- | :--- | :--- |
| `source_table` | `UTF8` | Originating database table (`customers`, `orders`, etc.). |
| `operation` | `UTF8` | CDC action (`c`, `u`, `d`, `r`). |
| `event_timestamp`| `INT64` | Origin event timestamp (ms from epoch). |
| `processing_timestamp` | `UTF8` | Pipeline ingestion timestamp (UTC ISO format). |
| `pipeline_run_id` | `UTF8` | UUID identifying the streaming micro-batch or batch DAG execution. |
| `validation_status` | `UTF8` | Literal `"INVALID"`. |
| `validation_reason` | `UTF8` | Explicit rule failure reason (e.g. `"total_amount is negative"`, `"invalid email"`). |
| `raw_payload` | `UTF8` | Serialized JSON representation of rejected event for auditing and replay. |

---

## 3. Gold Analytical Marts

### 3.1 `daily_sales`
- **Location:** `gold/daily_sales/`
- **Dimensions:** `order_date_dt`, `order_status`
- **Measures:** `total_orders`, `total_revenue`, `avg_order_value`, `cumulative_monthly_revenue`

### 3.2 `customer_lifetime_value`
- **Location:** `gold/customer_lifetime_value/`
- **Dimensions:** `customer_id`, `customer_name`, `email`, `city`, `state`
- **Measures:** `lifetime_orders`, `lifetime_value`, `avg_order_value`, `clv_rank`

### 3.3 `product_performance`
- **Location:** `gold/product_performance/`
- **Dimensions:** `product_id`, `product_name`, `category`
- **Measures:** `total_units_sold`, `gross_revenue`, `category_revenue_percentage`

### 3.4 `payment_summary`
- **Location:** `gold/payment_summary/`
- **Dimensions:** `payment_method`, `payment_status`
- **Measures:** `transaction_count`, `total_processed_volume`, `avg_transaction_value`, `success_rate_pct`
