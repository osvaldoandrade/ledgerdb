package paths

import (
	"fmt"
	"path/filepath"
	"strings"
)

func NormalizeRepoPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", ErrRepoPathRequired
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve repo path: %w", err)
	}

	return absPath, nil
}
