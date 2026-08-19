-- Compute end-to-end latency percentiles for completed tasks.
-- Run after a load test completes (pending count reaches 0).

SELECT
    count(*)                                                    AS total,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY completed_at - created_at) AS p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY completed_at - created_at) AS p95,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY completed_at - created_at) AS p99
FROM tasks
WHERE status = 'COMPLETED'
  AND type = 'realistic_work';
