package dhara

import "github.com/md-talim/dhara/dharatype"

// Errors returned by Client and Worker. Compare them with errors.Is.
var (
	// ErrTaskConflict is returned when an idempotency key is reused with a
	// different payload.
	ErrTaskConflict = dharatype.ErrTaskConflict

	// ErrTaskNotFound is returned when no task matches the given ID.
	ErrTaskNotFound = dharatype.ErrTaskNotFound

	// ErrTaskNotAvailable is returned by Claim when no task is ready.
	ErrTaskNotAvailable = dharatype.ErrTaskNotAvailable

	// ErrTaskOwnershipLost is returned when a worker tries to update a task
	// the reaper has already requeued.
	ErrTaskOwnershipLost = dharatype.ErrTaskOwnershipLost

	// ErrTaskNotDead is returned by RetryTask for a task that is not DEAD.
	ErrTaskNotDead = dharatype.ErrTaskNotDead
)

// ValidationError wraps invalid InsertParams or enqueue options. Use
// errors.As to inspect the cause.
type ValidationError = dharatype.ValidationError
