package inspect

import "errors"

var ErrHashRequired = errors.New("hash is required")
var ErrInvalidHash = errors.New("invalid git object hash")
var ErrBlobNotFound = errors.New("blob not found")
