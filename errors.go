package dhara

import "github.com/md-talim/dhara/dharatype"

var (
	ErrTaskConflict      = dharatype.ErrTaskConflict
	ErrTaskNotFound      = dharatype.ErrTaskNotFound
	ErrTaskNotAvailable  = dharatype.ErrTaskNotAvailable
	ErrTaskOwnershipLost = dharatype.ErrTaskOwnershipLost
	ErrTaskNotDead       = dharatype.ErrTaskNotDead
)

type ValidationError = dharatype.ValidationError
