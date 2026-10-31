// Package dhara is a PostgreSQL-backed task queue for Go.
//
// A Client enqueues tasks; a Worker claims and executes them. Postgres is the
// only dependency: task state, locking, retries and execution logs all live in
// the database.
//
// Enqueue a task:
//
//	client := dhara.NewClient(pool)
//	res, err := client.Enqueue(ctx, "send_email", payload)
//
// Run a worker:
//
//	w := dhara.NewWorker(pool, dhara.WithMaxWorkers(10))
//	w.RegisterHandler("send_email", handleEmail)
//	if err := w.Start(ctx); err != nil { // blocks until ctx is cancelled
//		log.Fatal(err)
//	}
//
// Delivery is at-least-once: a handler can run more than once after a retry,
// a stale-heartbeat reap, or a shutdown, so handlers must be idempotent.
// See the repository README for design notes and operational guidance.
package dhara
