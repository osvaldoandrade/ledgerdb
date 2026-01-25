package cli

import (
	"errors"
	"testing"

	docapp "github.com/osvaldoandrade/ledgerdb/internal/app/doc"
	indexapp "github.com/osvaldoandrade/ledgerdb/internal/app/index"
	maintenanceapp "github.com/osvaldoandrade/ledgerdb/internal/app/maintenance"
	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

func TestNormalizeError(t *testing.T) {
	tests := []struct {
		err      error
		wantCode int
		wantKind ErrorKind
	}{
		{err: docapp.ErrDocNotFound, wantCode: ExitNotFound, wantKind: KindNotFound},
		{err: docapp.ErrTxNotFound, wantCode: ExitNotFound, wantKind: KindNotFound},
		{err: domain.ErrHeadChanged, wantCode: ExitConflict, wantKind: KindConflict},
		{err: domain.ErrSyncConflict, wantCode: ExitConflict, wantKind: KindConflict},
		{err: indexapp.ErrCommitNotFound, wantCode: ExitConflict, wantKind: KindConflict},
		{err: indexapp.ErrMissingDocument, wantCode: ExitConflict, wantKind: KindConflict},
		{err: paths.ErrRepoPathRequired, wantCode: ExitInvalid, wantKind: KindValidation},
		{err: maintenanceapp.ErrInvalidThreshold, wantCode: ExitInvalid, wantKind: KindValidation},
		{err: maintenanceapp.ErrInvalidMax, wantCode: ExitInvalid, wantKind: KindValidation},
		{err: indexapp.ErrMergeCommitUnsupported, wantCode: ExitInvalid, wantKind: KindValidation},
		{err: indexapp.ErrPatchUnsupported, wantCode: ExitInvalid, wantKind: KindValidation},
		{err: indexapp.ErrInvalidInterval, wantCode: ExitInvalid, wantKind: KindValidation},
		{err: indexapp.ErrInvalidJitter, wantCode: ExitInvalid, wantKind: KindValidation},
		{err: docapp.ErrTxReferenceRequired, wantCode: ExitInvalid, wantKind: KindValidation},
		{err: errors.New("boom"), wantCode: ExitInternal, wantKind: KindInternal},
	}

	for _, tt := range tests {
		got := NormalizeError(tt.err)
		if got.Code != tt.wantCode {
			t.Fatalf("expected code %d, got %d for %v", tt.wantCode, got.Code, tt.err)
		}
		if got.Kind != tt.wantKind {
			t.Fatalf("expected kind %s, got %s for %v", tt.wantKind, got.Kind, tt.err)
		}
	}
}

func TestExitCode(t *testing.T) {
	if ExitCode(nil) != 0 {
		t.Fatalf("expected ExitCode(nil) == 0")
	}

	custom := ExitError{Code: 9, Kind: KindInternal, Message: "custom"}
	if ExitCode(custom) != 9 {
		t.Fatalf("expected ExitCode(custom) == 9")
	}
}
