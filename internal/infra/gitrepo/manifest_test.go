package gitrepo

import (
	"strings"
	"testing"

	"github.com/osvaldoandrade/ledgerdb/internal/domain"
)

func TestParseManifestWithoutAppliedMigrationsStaysBackwardCompat(t *testing.T) {
	raw := []byte(strings.Join([]string{
		"version: 2",
		"name: test",
		"stream_layout: sharded",
		"history_mode: append",
		"",
	}, "\n"))

	manifest, err := parseManifest(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if manifest.AppliedMigrations != nil {
		t.Fatalf("expected nil AppliedMigrations, got %v", manifest.AppliedMigrations)
	}
}

func TestRenderManifestOmitsEmptyAppliedMigrations(t *testing.T) {
	manifest := domain.Manifest{Version: 2, Name: "test", StreamLayout: domain.DefaultStreamLayout, HistoryMode: domain.DefaultHistoryMode}
	rendered := renderManifest(manifest)
	if strings.Contains(rendered, "applied_migrations") {
		t.Fatalf("did not expect applied_migrations key, got: %s", rendered)
	}
}

func TestManifestAppliedMigrationsRoundTrip(t *testing.T) {
	manifest := domain.Manifest{
		Version:           2,
		Name:              "test",
		StreamLayout:      domain.DefaultStreamLayout,
		HistoryMode:       domain.DefaultHistoryMode,
		AppliedMigrations: []string{"0001_users", "0002_orders"},
	}
	rendered := renderManifest(manifest)
	parsed, err := parseManifest([]byte(rendered))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(parsed.AppliedMigrations) != 2 || parsed.AppliedMigrations[0] != "0001_users" || parsed.AppliedMigrations[1] != "0002_orders" {
		t.Fatalf("round trip failed: %v", parsed.AppliedMigrations)
	}
}
