package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	collectionapp "github.com/osvaldoandrade/ledgerdb/internal/app/collection"
	docapp "github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	indexapp "github.com/osvaldoandrade/ledgerdb/internal/app/index"
	inspectapp "github.com/osvaldoandrade/ledgerdb/internal/app/inspect"
	maintenanceapp "github.com/osvaldoandrade/ledgerdb/internal/app/maintenance"
	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
	repoapp "github.com/osvaldoandrade/ledgerdb/internal/app/repo"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type ErrorKind string

const (
	KindInternal   ErrorKind = "internal"
	KindValidation ErrorKind = "validation"
	KindNotFound   ErrorKind = "not_found"
	KindConflict   ErrorKind = "conflict"
)

const (
	ExitInternal = 1
	ExitInvalid  = 2
	ExitNotFound = 3
	ExitConflict = 4
)

type ExitError struct {
	Code    int
	Kind    ErrorKind
	Message string
	Err     error
}

func (e ExitError) Error() string {
	return errorMessage(e)
}

func NormalizeError(err error) ExitError {
	if err == nil {
		return ExitError{Code: 0}
	}
	var exitErr ExitError
	if errors.As(err, &exitErr) {
		if exitErr.Code == 0 {
			exitErr.Code = ExitInternal
		}
		return exitErr
	}

	switch {
	case errors.Is(err, docapp.ErrDocNotFound),
		errors.Is(err, docapp.ErrDocDeleted),
		errors.Is(err, docapp.ErrTxNotFound),
		errors.Is(err, inspectapp.ErrBlobNotFound):
		return ExitError{Code: ExitNotFound, Kind: KindNotFound, Err: err}
	case errors.Is(err, domain.ErrHeadChanged),
		errors.Is(err, domain.ErrSyncConflict),
		errors.Is(err, indexapp.ErrCommitNotFound),
		errors.Is(err, indexapp.ErrMissingDocument):
		return ExitError{Code: ExitConflict, Kind: KindConflict, Err: err}
	case errors.Is(err, paths.ErrRepoPathRequired),
		errors.Is(err, repoapp.ErrRepoURLRequired),
		errors.Is(err, repoapp.ErrClonePathRequired),
		errors.Is(err, collectionapp.ErrCollectionRequired),
		errors.Is(err, collectionapp.ErrSchemaPathRequired),
		errors.Is(err, collectionapp.ErrInvalidCollectionName),
		errors.Is(err, collectionapp.ErrSchemaInvalidJSON),
		errors.Is(err, docapp.ErrCollectionRequired),
		errors.Is(err, docapp.ErrInvalidCollection),
		errors.Is(err, docapp.ErrDocIDRequired),
		errors.Is(err, docapp.ErrPayloadRequired),
		errors.Is(err, docapp.ErrTxReferenceRequired),
		errors.Is(err, docapp.ErrTxReferenceAmbiguous),
		errors.Is(err, inspectapp.ErrHashRequired),
		errors.Is(err, inspectapp.ErrInvalidHash),
		errors.Is(err, maintenanceapp.ErrInvalidThreshold),
		errors.Is(err, maintenanceapp.ErrInvalidMax),
		errors.Is(err, indexapp.ErrMergeCommitUnsupported),
		errors.Is(err, indexapp.ErrPatchUnsupported),
		errors.Is(err, indexapp.ErrInvalidInterval),
		errors.Is(err, indexapp.ErrInvalidJitter),
		errors.Is(err, domain.ErrTxIDRequired),
		errors.Is(err, domain.ErrTimestampRequired),
		errors.Is(err, domain.ErrCollectionRequired),
		errors.Is(err, domain.ErrDocIDRequired),
		errors.Is(err, domain.ErrInvalidOp),
		errors.Is(err, domain.ErrMissingPayload),
		errors.Is(err, domain.ErrUnexpectedPayload),
		errors.Is(err, domain.ErrMultiplePayloads):
		return ExitError{Code: ExitInvalid, Kind: KindValidation, Err: err}
	default:
		return ExitError{Code: ExitInternal, Kind: KindInternal, Err: err}
	}
}

func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	return NormalizeError(err).Code
}

func writeCLIError(w io.Writer, exitErr ExitError, asJSON bool) error {
	if exitErr.Code == 0 {
		return nil
	}
	message := errorMessage(exitErr)
	if asJSON {
		payload := struct {
			Code    int    `json:"code"`
			Kind    string `json:"kind"`
			Message string `json:"message"`
		}{
			Code:    exitErr.Code,
			Kind:    string(exitErr.Kind),
			Message: message,
		}
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(payload)
	}

	ui := newRenderer(w, false)
	prefix := "Error"
	if exitErr.Kind != "" {
		prefix = fmt.Sprintf("Error (%s)", exitErr.Kind)
	}
	prefix = ui.err(prefix)
	_, err := fmt.Fprintf(w, "%s: %s\n", prefix, message)
	return err
}

func errorMessage(exitErr ExitError) string {
	if exitErr.Message != "" {
		return exitErr.Message
	}
	if exitErr.Err != nil {
		return exitErr.Err.Error()
	}
	return "unknown error"
}
