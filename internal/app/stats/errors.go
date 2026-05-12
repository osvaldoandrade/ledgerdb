package stats

import "errors"

// ErrRefRequired is returned when a diff command receives an empty ref.
var ErrRefRequired = errors.New("ref is required")

// ErrInvalidLimit is returned when a non-positive limit is supplied.
var ErrInvalidLimit = errors.New("limit must be >= 0")
