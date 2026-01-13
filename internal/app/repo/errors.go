package repo

import "errors"

var ErrRepoURLRequired = errors.New("repo url is required")
var ErrClonePathRequired = errors.New("clone path is required")
