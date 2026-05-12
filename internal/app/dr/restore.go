package dr

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	integrityapp "github.com/osvaldoandrade/ledgerdb/internal/app/integrity"
)

// RestoreService reverses BackupService — it unpacks a tarball, restores the
// underlying bundle into a fresh bare repository, and validates integrity.
type RestoreService struct {
	restorer BundleRestorer
	verifier IntegrityVerifier
	clock    Clock
}

// NewRestoreService constructs a RestoreService.
func NewRestoreService(restorer BundleRestorer, verifier IntegrityVerifier, clock Clock) *RestoreService {
	return &RestoreService{restorer: restorer, verifier: verifier, clock: clock}
}

// Restore performs the restore. It rejects any tarball whose embedded bundle
// hash does not match the metadata.
func (s *RestoreService) Restore(ctx context.Context, opts RestoreOptions) (RestoreResult, error) {
	input := strings.TrimSpace(opts.InputPath)
	if input == "" {
		return RestoreResult{}, fmt.Errorf("%w: input path required", ErrInvalidBackup)
	}
	absInput, err := filepath.Abs(input)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("resolve input path: %w", err)
	}

	target := strings.TrimSpace(opts.TargetPath)
	if target == "" {
		target = fmt.Sprintf("restored-%s", s.clock.Now().UTC().Format("20060102T150405Z"))
	}
	absTarget, err := filepath.Abs(target)
	if err != nil {
		return RestoreResult{}, fmt.Errorf("resolve target path: %w", err)
	}

	if info, err := os.Stat(absTarget); err == nil && info.IsDir() {
		entries, err := os.ReadDir(absTarget)
		if err != nil {
			return RestoreResult{}, fmt.Errorf("read target dir: %w", err)
		}
		if len(entries) != 0 {
			return RestoreResult{}, fmt.Errorf("target %s is not empty", absTarget)
		}
	}

	stagingDir, err := os.MkdirTemp("", "ledgerdb-restore-")
	if err != nil {
		return RestoreResult{}, fmt.Errorf("create staging dir: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(stagingDir)
	}()

	manifest, bundlePath, repoManifest, err := extractBackup(absInput, stagingDir)
	if err != nil {
		return RestoreResult{}, err
	}
	if err := validateBundle(bundlePath, manifest); err != nil {
		return RestoreResult{}, err
	}

	if err := s.restorer.RestoreBundle(ctx, bundlePath, absTarget); err != nil {
		return RestoreResult{}, err
	}

	if len(repoManifest) > 0 {
		if err := os.WriteFile(filepath.Join(absTarget, "db.yaml"), repoManifest, 0o644); err != nil {
			return RestoreResult{}, fmt.Errorf("write restored manifest: %w", err)
		}
	}

	result := RestoreResult{TargetPath: absTarget, BundleHash: manifest.BundleHash}
	if opts.SkipVerify {
		result.IntegritySkipped = true
		return result, nil
	}

	verify, err := s.verifier.Verify(ctx, absTarget, integrityapp.VerifyOptions{Deep: true})
	if err != nil {
		return RestoreResult{}, fmt.Errorf("verify restored repo: %w", err)
	}
	result.Streams = verify.Streams
	result.IntegrityValid = verify.Valid
	if len(verify.Issues) > 0 {
		// best-effort cleanup so we don't leave a broken repo behind
		_ = os.RemoveAll(absTarget)
		return result, fmt.Errorf("%w: %d issue(s) in %d stream(s)",
			ErrIntegrityFailed, len(verify.Issues), verify.Streams)
	}

	return result, nil
}

func extractBackup(archivePath, dest string) (BackupManifest, string, []byte, error) {
	f, err := os.Open(archivePath)
	if err != nil {
		return BackupManifest{}, "", nil, fmt.Errorf("open backup: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return BackupManifest{}, "", nil, fmt.Errorf("%w: %v", ErrInvalidBackup, err)
	}
	defer func() {
		_ = gz.Close()
	}()

	tr := tar.NewReader(gz)
	var manifest BackupManifest
	var bundlePath string
	var manifestSeen bool
	var bundleSeen bool
	var repoManifest []byte

	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return BackupManifest{}, "", nil, fmt.Errorf("%w: read tar: %v", ErrInvalidBackup, err)
		}

		switch {
		case header.Name == BackupManifestName:
			data, err := io.ReadAll(tr)
			if err != nil {
				return BackupManifest{}, "", nil, fmt.Errorf("read backup manifest: %w", err)
			}
			if err := json.Unmarshal(data, &manifest); err != nil {
				return BackupManifest{}, "", nil, fmt.Errorf("%w: parse manifest: %v", ErrInvalidBackup, err)
			}
			manifestSeen = true
		case header.Name == BackupBundleName:
			bundlePath = filepath.Join(dest, BackupBundleName)
			out, err := os.Create(bundlePath)
			if err != nil {
				return BackupManifest{}, "", nil, fmt.Errorf("create bundle file: %w", err)
			}
			if _, err := io.Copy(out, tr); err != nil {
				_ = out.Close()
				return BackupManifest{}, "", nil, fmt.Errorf("write bundle file: %w", err)
			}
			if err := out.Close(); err != nil {
				return BackupManifest{}, "", nil, fmt.Errorf("close bundle file: %w", err)
			}
			bundleSeen = true
		case header.Name == BackupRepoManifestName:
			data, err := io.ReadAll(tr)
			if err != nil {
				return BackupManifest{}, "", nil, fmt.Errorf("read repo manifest: %w", err)
			}
			repoManifest = data
		default:
			// silently ignore sidecar and any future additions
			if _, err := io.Copy(io.Discard, tr); err != nil {
				return BackupManifest{}, "", nil, fmt.Errorf("skip entry %s: %w", header.Name, err)
			}
		}
	}

	if !manifestSeen {
		return BackupManifest{}, "", nil, fmt.Errorf("%w: missing %s", ErrInvalidBackup, BackupManifestName)
	}
	if !bundleSeen {
		return BackupManifest{}, "", nil, fmt.Errorf("%w: missing %s", ErrInvalidBackup, BackupBundleName)
	}
	if manifest.FormatVersion == 0 {
		return BackupManifest{}, "", nil, fmt.Errorf("%w: format_version missing", ErrInvalidBackup)
	}
	if manifest.FormatVersion > BackupFormatVersion {
		return BackupManifest{}, "", nil, fmt.Errorf("%w: unsupported format_version %d", ErrInvalidBackup, manifest.FormatVersion)
	}
	return manifest, bundlePath, repoManifest, nil
}

func validateBundle(bundlePath string, manifest BackupManifest) error {
	got, size, err := hashFile(bundlePath)
	if err != nil {
		return err
	}
	if got != manifest.BundleHash {
		return fmt.Errorf("%w: bundle hash mismatch (expected %s got %s)", ErrInvalidBackup, manifest.BundleHash, got)
	}
	if manifest.BundleSize != 0 && manifest.BundleSize != size {
		return fmt.Errorf("%w: bundle size mismatch", ErrInvalidBackup)
	}
	return nil
}
