-- =============================================================================
-- Analytical Query: Payment Performance & Method Success Rates
-- Answers: Payment failure rate, best payment method, daily volume
-- =============================================================================

SELECT
    payment_method,
    COUNT(payment_id) AS total_transactions,
    COUNT(CASE WHEN payment_status = 'SUCCESS' THEN 1 END) AS successful_transactions,
    COUNT(CASE WHEN payment_status = 'FAILED' THEN 1 END) AS failed_transactions,
    ROUND(
        COUNT(CASE WHEN payment_status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(payment_id),
        2
    ) AS success_rate_pct,
    ROUND(SUM(payment_amount), 2) AS total_processed_volume,
    ROUND(AVG(payment_amount), 2) AS avg_transaction_value
FROM payments_source
GROUP BY payment_method
ORDER BY total_processed_volume DESC;
