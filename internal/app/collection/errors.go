package collection

import "errors"

var ErrCollectionRequired = errors.New("collection name is required")
var ErrSchemaPathRequired = errors.New("schema path is required")
var ErrInvalidCollectionName = errors.New("invalid collection name")
var ErrSchemaInvalidJSON = errors.New("schema is not valid JSON")
