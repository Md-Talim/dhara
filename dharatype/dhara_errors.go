package dharatype

import "errors"

var (
	ErrTaskConflict      error = errors.New("dhara: task conflict: idempotency key reused with different payload")
	ErrTaskNotFound      error = errors.New("dhara: task not found")
	ErrTaskNotAvailable  error = errors.New("dhara: task not available")
	ErrTaskOwnershipLost error = errors.New("dhara: task ownership lost")
	ErrTaskNotDead       error = errors.New("dhara: task is not in DEAD status")
)

// ValidationError reports invalid insert parameters.
type ValidationError struct{ Err error }

func (e *ValidationError) Error() string { return e.Err.Error() }
func (e *ValidationError) Unwrap() error { return e.Err }
