-- Run this in a loop (see below) to capture drain progress with real timestamps.
-- Each row gives you: wall-clock time, completed count, pending count.
-- From this you can compute exact steady-state throughput (delta completed / delta time)
-- and exact drain time (time until pending hits 0).

SELECT
    now() AS checked_at,
    COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed,
    COUNT(*) FILTER (WHERE status = 'PENDING') AS pending
FROM tasks
WHERE type = 'realistic_work';
