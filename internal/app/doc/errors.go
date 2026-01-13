package doc

import "errors"

var ErrCollectionRequired = errors.New("collection is required")
var ErrInvalidCollection = errors.New("invalid collection name")
var ErrDocIDRequired = errors.New("doc id is required")
var ErrPayloadRequired = errors.New("payload is required")
var ErrDocNotFound = errors.New("document not found")
var ErrDocDeleted = errors.New("document deleted")
var ErrPatchUnsupported = errors.New("patch operations not supported")
var ErrTxReferenceRequired = errors.New("tx id or tx hash is required")
var ErrTxReferenceAmbiguous = errors.New("tx id and tx hash cannot be used together")
var ErrTxNotFound = errors.New("transaction not found")
