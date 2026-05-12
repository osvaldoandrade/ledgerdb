package dr

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeClock struct{ now time.Time }

func (f fakeClock) Now() time.Time { return f.now }

type fakeBundler struct{ payload []byte }

func (f *fakeBundler) Bundle(_ context.Context, _, bundlePath string) error {
	return os.WriteFile(bundlePath, f.payload, 0o644)
}

func TestBackupWritesManifestAndBundle(t *testing.T) {
	repoDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(repoDir, "db.yaml"), []byte("version: 2\nname: test\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(t.TempDir(), "backup.tar.gz")
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	svc := NewBackupService(&fakeBundler{payload: []byte("bundle-data")}, fakeClock{now: now})

	result, err := svc.Backup(context.Background(), repoDir, BackupOptions{OutputPath: out})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	if result.OutputPath != out {
		t.Fatalf("expected output %s, got %s", out, result.OutputPath)
	}

	expectedHash := sha256.Sum256([]byte("bundle-data"))
	if result.BundleHash != hex.EncodeToString(expectedHash[:]) {
		t.Fatalf("bundle hash mismatch: %s", result.BundleHash)
	}

	files := readTarball(t, out)
	if string(files[BackupBundleName]) != "bundle-data" {
		t.Fatalf("bundle payload not preserved")
	}
	if _, ok := files[BackupRepoManifestName]; !ok {
		t.Fatalf("expected db.yaml in archive")
	}

	manifestBytes, ok := files[BackupManifestName]
	if !ok {
		t.Fatalf("expected backup manifest in archive")
	}
	var manifest BackupManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if manifest.FormatVersion != BackupFormatVersion {
		t.Fatalf("expected format version %d, got %d", BackupFormatVersion, manifest.FormatVersion)
	}
	if manifest.BundleHash != result.BundleHash {
		t.Fatalf("manifest hash mismatch")
	}
	if manifest.LedgerDBVersion == "" {
		t.Fatalf("expected ledgerdb version in manifest")
	}
}

func readTarball(t *testing.T, path string) map[string][]byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	out := map[string][]byte{}
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		out[h.Name] = data
	}
	return out
}
