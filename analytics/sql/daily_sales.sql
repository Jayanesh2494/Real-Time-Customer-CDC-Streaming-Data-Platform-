-- =============================================================================
-- Analytical Query: Daily & Monthly Sales Analysis
-- Answers: What is the daily and monthly revenue? What is the trend?
-- =============================================================================

WITH cleaned_orders AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        total_amount,
        CAST(order_date AS DATE) AS order_dt,
        STRFTIME(CAST(order_date AS DATE), '%Y-%m') AS order_month
    FROM orders_source
    WHERE order_status != 'CANCELLED'
)
SELECT
    order_month,
    order_dt,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(total_amount), 2) AS daily_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    ROUND(SUM(SUM(total_amount)) OVER (
        PARTITION BY order_month
        ORDER BY order_dt
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS cumulative_monthly_revenue
FROM cleaned_orders
GROUP BY order_month, order_dt
ORDER BY order_dt DESC;
