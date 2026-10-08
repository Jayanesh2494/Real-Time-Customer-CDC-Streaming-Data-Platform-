-- =============================================================================
-- Analytical Query: Customer Lifetime Value (CLV) & City Distribution
-- Answers: Which customers spend the most? Which cities generate the highest revenue?
-- =============================================================================

WITH customer_spending AS (
    SELECT
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        c.email,
        c.city,
        c.state,
        COUNT(o.order_id) AS total_orders,
        ROUND(COALESCE(SUM(o.total_amount), 0.0), 2) AS lifetime_value,
        ROUND(COALESCE(AVG(o.total_amount), 0.0), 2) AS avg_order_value,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS latest_order_date
    FROM customers_source c
    LEFT JOIN orders_source o 
        ON c.customer_id = o.customer_id 
        AND o.order_status != 'CANCELLED'
    GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.state
)
SELECT
    customer_id,
    customer_name,
    email,
    city,
    state,
    total_orders,
    lifetime_value,
    avg_order_value,
    DENSE_RANK() OVER (ORDER BY lifetime_value DESC) AS clv_rank
FROM customer_spending
ORDER BY lifetime_value DESC
LIMIT 20;
