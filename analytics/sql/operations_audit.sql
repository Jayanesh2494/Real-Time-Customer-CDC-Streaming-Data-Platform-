-- =============================================================================
-- Analytical Query: Operational Audit & Dead-Letter Queue (DLQ) Analysis
-- Answers: Error counts, error rate by entity, failed validation reasons
-- =============================================================================

SELECT
    source_table,
    validation_status,
    validation_reason,
    COUNT(*) AS incident_count,
    MIN(processing_timestamp) AS first_seen,
    MAX(processing_timestamp) AS last_seen
FROM errors_source
GROUP BY source_table, validation_status, validation_reason
ORDER BY incident_count DESC;
