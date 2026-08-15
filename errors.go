package dhara

import (
	"errors"

	"github.com/md-talim/dhara/internal/store"
)

var (
	ErrTaskConflict      = errors.New("dhara: task conflict: idempotency key reused with different payload")
	ErrTaskNotFound      = errors.New("dhara: task not found")
	ErrTaskNotAvailable  = errors.New("dhara: task not available")
	ErrTaskOwnershipLost = errors.New("dhara: task ownership lost")
	ErrTaskNotDead       = errors.New("dhara: task is not in DEAD status")
)

// ValidationError reports invalid insert parameters.
type ValidationError struct{ Err error }

func (e *ValidationError) Error() string { return e.Err.Error() }
func (e *ValidationError) Unwrap() error { return e.Err }

func mapStoreError(err error) error {
	switch {
	case errors.Is(err, store.ErrTaskConflict):
		return ErrTaskConflict
	case errors.Is(err, store.ErrTaskNotFound):
		return ErrTaskNotFound
	case errors.Is(err, store.ErrTaskNotAvailable):
		return ErrTaskNotAvailable
	case errors.Is(err, store.ErrTaskOwnershipLost):
		return ErrTaskOwnershipLost
	case errors.Is(err, store.ErrTaskNotDead):
		return ErrTaskNotDead
	default:
		return err
	}
}
