package index

import "errors"

var ErrCommitNotFound = errors.New("last indexed commit not found in repo")
var ErrMergeCommitUnsupported = errors.New("merge commits are not supported")
var ErrFetchUnavailable = errors.New("fetch is not configured")
var ErrMissingDocument = errors.New("document missing for patch")
var ErrPatchUnsupported = errors.New("patch operations not supported")
var ErrStateUnavailable = errors.New("state tree not available")
var ErrInvalidInterval = errors.New("invalid sync interval")
var ErrInvalidJitter = errors.New("invalid sync jitter")
var ErrInvalidBatchCommits = errors.New("invalid commit batch size")
