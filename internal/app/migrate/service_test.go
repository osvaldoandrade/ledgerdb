package migrate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

type fakeApplier struct {
	calls []applyCall
	err   error
}

type applyCall struct {
	collection string
	indexes    []domain.IndexSpec
}

func (f *fakeApplier) Apply(_ context.Context, _, collection, _ string, indexes []domain.IndexSpec) error {
	f.calls = append(f.calls, applyCall{collection: collection, indexes: append([]domain.IndexSpec(nil), indexes...)})
	return f.err
}

type fakeManifest struct {
	manifest domain.Manifest
	writes   int
}

func (f *fakeManifest) LoadManifest(_ string) (domain.Manifest, error) {
	return f.manifest, nil
}

func (f *fakeManifest) WriteManifest(_ context.Context, _ string, manifest domain.Manifest) error {
	f.manifest = manifest
	f.writes++
	return nil
}

func writeMigration(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatalf("write migration: %v", err)
	}
}

func TestStatusReportsApplyAndPending(t *testing.T) {
	repo := t.TempDir()
	writeMigration(t, filepath.Join(repo, "migrations"), "0001_users.json", `{"collection":"users","schema":{"type":"object"}}`)
	writeMigration(t, filepath.Join(repo, "migrations"), "0002_orders.json", `{"collection":"orders","schema":{"type":"object"}}`)

	manifest := &fakeManifest{manifest: domain.Manifest{AppliedMigrations: []string{"0001_users"}}}
	svc := NewService(&fakeApplier{}, manifest)

	res, err := svc.Status(context.Background(), repo)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(res.Applied) != 1 || res.Applied[0] != "0001_users" {
		t.Fatalf("applied: %v", res.Applied)
	}
	if len(res.Pending) != 1 || res.Pending[0] != "0002_orders" {
		t.Fatalf("pending: %v", res.Pending)
	}
}

func TestApplyRunsInOrderAndUpdatesManifest(t *testing.T) {
	repo := t.TempDir()
	writeMigration(t, filepath.Join(repo, "migrations"), "0002_orders.json", `{"collection":"orders","schema":{"type":"object"}}`)
	writeMigration(t, filepath.Join(repo, "migrations"), "0001_users.json", `{"collection":"users","schema":{"type":"object"},"indexes":["id"]}`)

	applier := &fakeApplier{}
	manifest := &fakeManifest{}
	svc := NewService(applier, manifest)

	res, err := svc.Apply(context.Background(), repo, ApplyOptions{})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(res.Applied) != 2 || res.Applied[0] != "0001_users" || res.Applied[1] != "0002_orders" {
		t.Fatalf("applied order: %v", res.Applied)
	}
	if len(applier.calls) != 2 || applier.calls[0].collection != "users" || applier.calls[1].collection != "orders" {
		t.Fatalf("applier calls: %+v", applier.calls)
	}
	if len(applier.calls[0].indexes) != 1 || applier.calls[0].indexes[0].Name != "id" ||
		len(applier.calls[0].indexes[0].Fields) != 1 || applier.calls[0].indexes[0].Fields[0] != "id" {
		t.Fatalf("indexes not forwarded: %+v", applier.calls[0].indexes)
	}
	if manifest.writes != 2 {
		t.Fatalf("expected 2 manifest writes, got %d", manifest.writes)
	}
	if len(manifest.manifest.AppliedMigrations) != 2 {
		t.Fatalf("manifest state: %v", manifest.manifest.AppliedMigrations)
	}
}

func TestApplyIdempotent(t *testing.T) {
	repo := t.TempDir()
	writeMigration(t, filepath.Join(repo, "migrations"), "0001_users.json", `{"collection":"users","schema":{"type":"object"}}`)

	applier := &fakeApplier{}
	manifest := &fakeManifest{manifest: domain.Manifest{AppliedMigrations: []string{"0001_users"}}}
	svc := NewService(applier, manifest)

	res, err := svc.Apply(context.Background(), repo, ApplyOptions{})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(res.Applied) != 0 {
		t.Fatalf("expected no applied, got %v", res.Applied)
	}
	if len(applier.calls) != 0 {
		t.Fatalf("expected no applier calls, got %d", len(applier.calls))
	}
}

func TestApplyDryRunDoesNotMutate(t *testing.T) {
	repo := t.TempDir()
	writeMigration(t, filepath.Join(repo, "migrations"), "0001_users.json", `{"collection":"users","schema":{"type":"object"}}`)

	applier := &fakeApplier{}
	manifest := &fakeManifest{}
	svc := NewService(applier, manifest)

	res, err := svc.Apply(context.Background(), repo, ApplyOptions{DryRun: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !res.DryRun {
		t.Fatalf("expected dry run flag")
	}
	if len(res.Pending) != 1 {
		t.Fatalf("pending: %v", res.Pending)
	}
	if len(applier.calls) != 0 || manifest.writes != 0 {
		t.Fatalf("dry run mutated state: calls=%d writes=%d", len(applier.calls), manifest.writes)
	}
}

func TestApplyTargetTruncates(t *testing.T) {
	repo := t.TempDir()
	writeMigration(t, filepath.Join(repo, "migrations"), "0001_a.json", `{"collection":"a","schema":{"type":"object"}}`)
	writeMigration(t, filepath.Join(repo, "migrations"), "0002_b.json", `{"collection":"b","schema":{"type":"object"}}`)
	writeMigration(t, filepath.Join(repo, "migrations"), "0003_c.json", `{"collection":"c","schema":{"type":"object"}}`)

	applier := &fakeApplier{}
	manifest := &fakeManifest{}
	svc := NewService(applier, manifest)

	res, err := svc.Apply(context.Background(), repo, ApplyOptions{Target: 2})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(res.Applied) != 2 {
		t.Fatalf("applied: %v", res.Applied)
	}
	if applier.calls[len(applier.calls)-1].collection != "b" {
		t.Fatalf("last applier call: %+v", applier.calls)
	}
}

func TestApplyRejectsUnknownTarget(t *testing.T) {
	repo := t.TempDir()
	writeMigration(t, filepath.Join(repo, "migrations"), "0001_a.json", `{"collection":"a","schema":{"type":"object"}}`)

	svc := NewService(&fakeApplier{}, &fakeManifest{})
	_, err := svc.Apply(context.Background(), repo, ApplyOptions{Target: 99})
	if !errors.Is(err, ErrTargetNotFound) {
		t.Fatalf("expected ErrTargetNotFound, got %v", err)
	}
}

func TestDiscoverRejectsInvalidFilenames(t *testing.T) {
	repo := t.TempDir()
	writeMigration(t, filepath.Join(repo, "migrations"), "bogus.json", `{}`)

	svc := NewService(&fakeApplier{}, &fakeManifest{})
	_, err := svc.Status(context.Background(), repo)
	if !errors.Is(err, ErrInvalidMigrationName) {
		t.Fatalf("expected ErrInvalidMigrationName, got %v", err)
	}
}

func TestStatusOnMissingMigrationsDir(t *testing.T) {
	repo := t.TempDir()
	svc := NewService(&fakeApplier{}, &fakeManifest{})

	res, err := svc.Status(context.Background(), repo)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(res.Applied) != 0 || len(res.Pending) != 0 {
		t.Fatalf("expected empty status, got %+v", res)
	}
}
