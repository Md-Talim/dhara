# Benchmark Results

Last run: 2026-08-18

## Environment

- CPU: Intel i3, 11th gen
- RAM: 8GB
- Disk: 500GB SSD
- Postgres: local, via `docker compose`
- Load generator: [k6](https://k6.io)
- Handler under test: `realistic_work` -> simulates I/O with a randomized 50-200ms delay.

## Configuration

| Setting              | Value                              |
| :------------------- | :--------------------------------- |
| `WORKER_COUNT`       | 20                                 |
| `DHARA_MAX_CONNS`    | 25                                 |
| `POLL_INTERVAL`      | 1s (idle-only after the fix below) |
| `HEARTBEAT_INTERVAL` | 30s                                |

## Timeline

### 1. Initial prediction vs. measurement

Predicted sustained completion throughput: `WORKER_COUNT ÷ avg_handler_duration` = `20 ÷ ~0.135s` ≈ **160 tasks/sec**.

First load test: 120 tasks/sec submission for 60s (7,200 tasks total). Measured completion throughput, sampled via 5-second polling against task state:

```
100 completions / 5.08s ≈ 19.7 tasks/sec
```

Confirmed stable across two independent runs (multiple consecutive 5-second windows, each landing at exactly 100 completions). ~8x below prediction.

### 2. Root cause

Worker claim loop (`internal/queue/worker.go`, `Start`):

```go
for {
    select {
    case <-ctx.Done():
        return
    case <-ticker.C:
        w.processNext(ctx)
    }
}
```

One `processNext` call per ticker tick. After completing a task, the loop returned to waiting for the next tick rather than attempting another claim immediately, capping each worker at one claim attempt per `POLL_INTERVAL`, regardless of actual handler duration.

`20 workers × 1 claim/sec = 20/sec`: matches the measured ~19.7/sec almost exactly.

### 3. Fix

Changed the loop to claim continuously while work is available, falling back to the poll interval only when a claim attempt finds nothing (`store.ErrTaskNotAvailable`):

```go
for {
	task, err := w.claim(ctx)
	if err != nil {
		// No work available or context cancelled: wait before trying again.
		select {
			case <-ticker.C:
			continue
			case <-ctx.Done():
			return
		}
	}

	// Got a task: process it immediately, then loop back to claim again.
	if err := w.execute(ctx, task); err != nil {
		w.logger.Error("execute task failed", "err", err)
	}
}
```

### 4. Post-fix measurement

Same load test (120/sec submission, 20 workers). Four consecutive 5-second polling windows:

| Window | Completed delta | Duration | Rate    |
| :----- | :-------------- | :------- | :------ |
| 1      | 765             | 5.089s   | 150.3/s |
| 2      | 757             | 5.106s   | 148.3/s |
| 3      | 748             | 5.097s   | 146.7/s |
| 4      | 744             | 5.099s   | 145.9/s |

Average: **~147.8 tasks/sec**, within ~8% of the original 160/sec theoretical prediction.

### 5. Latency at two utilization points

**Headroom (100 tasks/sec submission, ~67% of measured capacity):**

```sql
-- 6,001 tasks, 0 not completed
p50: 0.180991s   p95: 0.298581s   p99: 0.379737s
```

**Saturation (~148 tasks/sec submission, ~100% of measured capacity):**

```sql
-- 9,001 tasks, 0 not completed
p50: 1.299964s   p95: 2.028858s   p99: 2.060421s
```

### 6. Burst / recovery (pre-fix, kept for reference)

Before the fix above, submitting 120/sec against a ~20/sec ceiling for 60s (7,200 tasks) produced a backlog peaking around 5,800 pending tasks. Full drain completed with **zero task loss**, all 7,200 tasks eventually reached `COMPLETED`. Latency percentiles from that run (p50 150s, p99 297s) reflect queueing delay from the deliberate 6x oversubmission, not per-task processing time, and aren't representative of normal operation (included here only as evidence of correct recovery behavior under sustained overload).

## Reproducing

```bash
# terminal 1
make dev

# terminal 2
k6 run benchmarks/load-test.js

# terminal 3, started alongside the k6 run
watch -n 5 'psql "$DHARA_DATABASE_URL" -f benchmarks/drain-check.sql'

# after k6 finishes and pending reaches 0, get percentiles:
psql "$DHARA_DATABASE_URL" -f benchmarks/percentiles.sql
```

Adjust `rate` in `load-test.js` to reproduce either the headroom or saturation scenario.
