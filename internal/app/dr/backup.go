package dr

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/osvaldoandrade/ledgerdb/internal/app/paths"
)

// BackupService writes a single tarball containing a git bundle plus metadata.
type BackupService struct {
	bundler Bundler
	clock   Clock
}

// NewBackupService constructs a BackupService.
func NewBackupService(bundler Bundler, clock Clock) *BackupService {
	return &BackupService{bundler: bundler, clock: clock}
}

// Backup writes a tarball at opts.OutputPath. The repo at repoPath is bundled
// via the configured Bundler, then the bundle, db.yaml, and a backup.json
// metadata file are packed into a gzipped tar archive.
func (s *BackupService) Backup(ctx context.Context, repoPath string, opts BackupOptions) (BackupResult, error) {
	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return BackupResult{}, err
	}

	output := strings.TrimSpace(opts.OutputPath)
	if output == "" {
		output = defaultBackupName(s.clock.Now())
	}
	absOutput, err := filepath.Abs(output)
	if err != nil {
		return BackupResult{}, fmt.Errorf("resolve output path: %w", err)
	}

	stagingDir, err := os.MkdirTemp("", "ledgerdb-backup-")
	if err != nil {
		return BackupResult{}, fmt.Errorf("create staging dir: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(stagingDir)
	}()

	bundlePath := filepath.Join(stagingDir, BackupBundleName)
	if err := s.bundler.Bundle(ctx, absRepoPath, bundlePath); err != nil {
		return BackupResult{}, err
	}

	bundleHash, bundleSize, err := hashFile(bundlePath)
	if err != nil {
		return BackupResult{}, err
	}

	version := strings.TrimSpace(opts.LedgerDBVersion)
	if version == "" {
		version = Version
	}

	manifest := BackupManifest{
		FormatVersion:   BackupFormatVersion,
		CreatedAt:       s.clock.Now().UTC(),
		SourceRepoPath:  absRepoPath,
		BundleHash:      bundleHash,
		BundleSize:      bundleSize,
		LedgerDBVersion: version,
		IncludesSidecar: opts.IncludeSidecar && opts.SidecarPath != "",
	}

	if err := os.MkdirAll(filepath.Dir(absOutput), 0o755); err != nil {
		return BackupResult{}, fmt.Errorf("create output directory: %w", err)
	}

	if err := writeBackupTar(absOutput, absRepoPath, bundlePath, manifest, opts); err != nil {
		return BackupResult{}, err
	}

	return BackupResult{
		OutputPath:      absOutput,
		BundleSize:      bundleSize,
		BundleHash:      bundleHash,
		CreatedAt:       manifest.CreatedAt,
		IncludesSidecar: manifest.IncludesSidecar,
	}, nil
}

func defaultBackupName(now time.Time) string {
	return fmt.Sprintf("ledgerdb-backup-%s.tar.gz", now.UTC().Format("20060102T150405Z"))
}

func writeBackupTar(outputPath, repoPath, bundlePath string, manifest BackupManifest, opts BackupOptions) error {
	out, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create backup tarball: %w", err)
	}
	defer func() {
		_ = out.Close()
	}()

	gz := gzip.NewWriter(out)
	defer func() {
		_ = gz.Close()
	}()

	tw := tar.NewWriter(gz)
	defer func() {
		_ = tw.Close()
	}()

	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode backup manifest: %w", err)
	}
	if err := addTarFile(tw, BackupManifestName, manifestBytes, 0o644); err != nil {
		return err
	}

	bundleBytes, err := os.ReadFile(bundlePath)
	if err != nil {
		return fmt.Errorf("read bundle: %w", err)
	}
	if err := addTarFile(tw, BackupBundleName, bundleBytes, 0o644); err != nil {
		return err
	}

	repoManifestPath := filepath.Join(repoPath, "db.yaml")
	if data, err := os.ReadFile(repoManifestPath); err == nil {
		if err := addTarFile(tw, BackupRepoManifestName, data, 0o644); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read repo manifest: %w", err)
	}

	if manifest.IncludesSidecar && opts.SidecarPath != "" {
		data, err := os.ReadFile(opts.SidecarPath)
		if err != nil {
			return fmt.Errorf("read sidecar: %w", err)
		}
		name := filepath.Join(SidecarDirName, filepath.Base(opts.SidecarPath))
		if err := addTarFile(tw, name, data, 0o644); err != nil {
			return err
		}
	}

	return nil
}

func addTarFile(tw *tar.Writer, name string, data []byte, mode int64) error {
	header := &tar.Header{
		Name: name,
		Mode: mode,
		Size: int64(len(data)),
	}
	if err := tw.WriteHeader(header); err != nil {
		return fmt.Errorf("write tar header %s: %w", name, err)
	}
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("write tar body %s: %w", name, err)
	}
	return nil
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, fmt.Errorf("open file for hashing: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("hash file: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}
