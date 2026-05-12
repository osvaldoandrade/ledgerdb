// Package migrate hosts ordered schema migration use cases.
package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

// ErrInvalidMigrationName is returned when a migration filename does not match NNNN_slug.json.
var ErrInvalidMigrationName = errors.New("invalid migration filename (expected NNNN_slug.json)")

// ErrTargetNotFound is returned when --target references an unknown migration number.
var ErrTargetNotFound = errors.New("target migration not found")

// ErrAppliedMissing is returned when a previously-applied migration is missing on disk.
var ErrAppliedMissing = errors.New("applied migration missing from migrations directory")

// migrationFilePattern matches NNNN_slug.json.
var migrationFilePattern = regexp.MustCompile(`^([0-9]{4,})_([A-Za-z0-9][A-Za-z0-9_-]*)\.json$`)

// CollectionApplier executes a single migration step (collection apply).
type CollectionApplier interface {
	Apply(ctx context.Context, repoPath, collection, schemaPath string, indexes []domain.IndexSpec) error
}

// ManifestStore reads and writes the repository manifest.
type ManifestStore interface {
	LoadManifest(repoPath string) (domain.Manifest, error)
	WriteManifest(ctx context.Context, repoPath string, manifest domain.Manifest) error
}

// Service runs and reports on ordered migrations.
type Service struct {
	applier  CollectionApplier
	manifest ManifestStore
}

// NewService constructs a migration Service.
func NewService(applier CollectionApplier, manifest ManifestStore) *Service {
	return &Service{applier: applier, manifest: manifest}
}

// Migration is a single migration file descriptor.
type Migration struct {
	Name     string // e.g. "0001_users"
	Number   int    // 1
	Slug     string // "users"
	FullPath string // absolute path to the .json file
}

// MigrationPayload is the on-disk shape for a migration file.
type MigrationPayload struct {
	Collection string          `json:"collection"`
	Schema     json.RawMessage `json:"schema"`
	Indexes    []string        `json:"indexes,omitempty"`
}

// ApplyOptions controls Apply behaviour.
type ApplyOptions struct {
	Target int  // 0 = apply all pending
	DryRun bool // do not mutate state
}

// ApplyResult summarizes an Apply run.
type ApplyResult struct {
	Applied []string `json:"applied"`
	Pending []string `json:"pending"`
	DryRun  bool     `json:"dry_run"`
}

// StatusResult summarizes migration status.
type StatusResult struct {
	Applied []string `json:"applied"`
	Pending []string `json:"pending"`
}

// Apply runs pending migrations in order.
func (s *Service) Apply(ctx context.Context, repoPath string, opts ApplyOptions) (ApplyResult, error) {
	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return ApplyResult{}, err
	}

	manifest, err := s.manifest.LoadManifest(absRepoPath)
	if err != nil {
		return ApplyResult{}, err
	}

	migrations, err := discoverMigrations(absRepoPath)
	if err != nil {
		return ApplyResult{}, err
	}

	applied := append([]string(nil), manifest.AppliedMigrations...)
	appliedSet := stringSet(applied)
	if err := verifyAppliedExist(appliedSet, migrations); err != nil {
		return ApplyResult{}, err
	}

	pending := filterPending(migrations, appliedSet)

	if opts.Target > 0 {
		pending, err = truncateAtTarget(pending, opts.Target, migrations, appliedSet)
		if err != nil {
			return ApplyResult{}, err
		}
	}

	result := ApplyResult{DryRun: opts.DryRun}

	if opts.DryRun {
		for _, m := range pending {
			result.Pending = append(result.Pending, m.Name)
		}
		return result, nil
	}

	for _, m := range pending {
		if err := s.applyOne(ctx, absRepoPath, m); err != nil {
			return result, fmt.Errorf("apply %s: %w", m.Name, err)
		}

		applied = append(applied, m.Name)
		result.Applied = append(result.Applied, m.Name)

		manifest.AppliedMigrations = applied
		if err := s.manifest.WriteManifest(ctx, absRepoPath, manifest); err != nil {
			return result, fmt.Errorf("persist manifest after %s: %w", m.Name, err)
		}
	}

	return result, nil
}

// Status reports applied vs pending migrations.
func (s *Service) Status(_ context.Context, repoPath string) (StatusResult, error) {
	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return StatusResult{}, err
	}

	manifest, err := s.manifest.LoadManifest(absRepoPath)
	if err != nil {
		return StatusResult{}, err
	}

	migrations, err := discoverMigrations(absRepoPath)
	if err != nil {
		return StatusResult{}, err
	}

	appliedSet := stringSet(manifest.AppliedMigrations)
	pending := filterPending(migrations, appliedSet)
	pendingNames := make([]string, 0, len(pending))
	for _, m := range pending {
		pendingNames = append(pendingNames, m.Name)
	}

	applied := append([]string(nil), manifest.AppliedMigrations...)
	if applied == nil {
		applied = []string{}
	}
	if pendingNames == nil {
		pendingNames = []string{}
	}

	return StatusResult{Applied: applied, Pending: pendingNames}, nil
}

func (s *Service) applyOne(ctx context.Context, repoPath string, m Migration) error {
	data, err := os.ReadFile(m.FullPath)
	if err != nil {
		return fmt.Errorf("read migration: %w", err)
	}

	var payload MigrationPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("parse migration: %w", err)
	}

	collection := strings.TrimSpace(payload.Collection)
	if collection == "" {
		return fmt.Errorf("migration %s: collection is required", m.Name)
	}
	if len(payload.Schema) == 0 {
		return fmt.Errorf("migration %s: schema is required", m.Name)
	}

	tmp, err := os.CreateTemp("", "ledgerdb-migrate-*.json")
	if err != nil {
		return fmt.Errorf("create temp schema: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(payload.Schema); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp schema: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp schema: %w", err)
	}

	specs := make([]domain.IndexSpec, 0, len(payload.Indexes))
	for _, field := range payload.Indexes {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		specs = append(specs, domain.IndexSpec{Name: field, Fields: []string{field}})
	}

	return s.applier.Apply(ctx, repoPath, collection, tmpPath, specs)
}

func discoverMigrations(repoPath string) ([]Migration, error) {
	dir := filepath.Join(repoPath, "migrations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}

	migrations := make([]Migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		matches := migrationFilePattern.FindStringSubmatch(name)
		if matches == nil {
			if strings.HasSuffix(name, ".json") {
				return nil, fmt.Errorf("%w: %s", ErrInvalidMigrationName, name)
			}
			continue
		}
		number, err := strconv.Atoi(matches[1])
		if err != nil {
			return nil, fmt.Errorf("parse migration number %s: %w", name, err)
		}
		migrations = append(migrations, Migration{
			Name:     strings.TrimSuffix(name, ".json"),
			Number:   number,
			Slug:     matches[2],
			FullPath: filepath.Join(dir, name),
		})
	}

	sort.Slice(migrations, func(i, j int) bool {
		if migrations[i].Number != migrations[j].Number {
			return migrations[i].Number < migrations[j].Number
		}
		return migrations[i].Name < migrations[j].Name
	})

	return migrations, nil
}

func stringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, v := range values {
		set[v] = struct{}{}
	}
	return set
}

func filterPending(migrations []Migration, applied map[string]struct{}) []Migration {
	pending := make([]Migration, 0, len(migrations))
	for _, m := range migrations {
		if _, ok := applied[m.Name]; ok {
			continue
		}
		pending = append(pending, m)
	}
	return pending
}

func verifyAppliedExist(applied map[string]struct{}, migrations []Migration) error {
	known := make(map[string]struct{}, len(migrations))
	for _, m := range migrations {
		known[m.Name] = struct{}{}
	}
	for name := range applied {
		if _, ok := known[name]; !ok {
			return fmt.Errorf("%w: %s", ErrAppliedMissing, name)
		}
	}
	return nil
}

func truncateAtTarget(pending []Migration, target int, all []Migration, applied map[string]struct{}) ([]Migration, error) {
	found := false
	for _, m := range all {
		if m.Number == target {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("%w: %04d", ErrTargetNotFound, target)
	}

	truncated := make([]Migration, 0, len(pending))
	for _, m := range pending {
		if m.Number > target {
			break
		}
		truncated = append(truncated, m)
	}
	return truncated, nil
}
