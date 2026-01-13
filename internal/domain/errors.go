package domain

import "errors"

var ErrHeadChanged = errors.New("stream head changed")
var ErrSyncConflict = errors.New("remote ahead; sync required")
