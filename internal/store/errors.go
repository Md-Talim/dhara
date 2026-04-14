package store

import "errors"

var ErrTaskConflict = errors.New("task conflict: idempotency key reused with different payload")
var ErrTaskNotFound = errors.New("task not found")
var ErrTaskNotAvailable = errors.New("task not available")
var ErrTaskOwnershipLost = errors.New("task ownership lost")
var ErrTaskNotDead = errors.New("task is not in DEAD status")
