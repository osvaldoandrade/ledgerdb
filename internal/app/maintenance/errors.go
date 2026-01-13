package maintenance

import "errors"

var ErrInvalidThreshold = errors.New("threshold must be greater than zero")
var ErrInvalidMax = errors.New("max must be zero or greater")
