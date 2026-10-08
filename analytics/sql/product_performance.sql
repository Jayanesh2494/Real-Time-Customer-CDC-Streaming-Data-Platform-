-- =============================================================================
-- Analytical Query: Product Performance & Category Contribution
-- Answers: Top selling products by units & gross revenue, category breakdown
-- =============================================================================

WITH item_sales AS (
    SELECT
        product_id,
        SUM(quantity) AS units_sold,
        ROUND(SUM(quantity * unit_price), 2) AS total_revenue
    FROM order_items_source
    GROUP BY product_id
)
SELECT
    p.category,
    p.product_id,
    p.product_name,
    p.price,
    p.stock_quantity,
    COALESCE(s.units_sold, 0) AS total_units_sold,
    COALESCE(s.total_revenue, 0.0) AS gross_revenue,
    ROUND(
        COALESCE(s.total_revenue, 0.0) * 100.0 / NULLIF(SUM(s.total_revenue) OVER (PARTITION BY p.category), 0),
        2
    ) AS category_revenue_percentage
FROM products_source p
LEFT JOIN item_sales s ON p.product_id = s.product_id
ORDER BY gross_revenue DESC;
