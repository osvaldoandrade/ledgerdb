package gitrepo

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/codecompany/ledgerdb/internal/domain"
)

func renderManifest(manifest domain.Manifest) string {
	manifest = manifest.WithDefaults()
	createdAt := ""
	if !manifest.CreatedAt.IsZero() {
		createdAt = manifest.CreatedAt.UTC().Format(time.RFC3339Nano)
	}

	var builder strings.Builder
	builder.WriteString("version: ")
	builder.WriteString(fmt.Sprintf("%d", manifest.Version))
	builder.WriteString("\n")
	builder.WriteString("name: ")
	builder.WriteString(manifest.Name)
	builder.WriteString("\n")
	builder.WriteString("stream_layout: ")
	builder.WriteString(string(manifest.StreamLayout))
	builder.WriteString("\n")
	builder.WriteString("history_mode: ")
	builder.WriteString(string(manifest.HistoryMode))
	builder.WriteString("\n")
	if createdAt != "" {
		builder.WriteString("created_at: ")
		builder.WriteString(createdAt)
		builder.WriteString("\n")
	}
	return builder.String()
}

func parseManifest(data []byte) (domain.Manifest, error) {
	lines := strings.Split(string(data), "\n")
	manifest := domain.Manifest{}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch key {
		case "version":
			version, err := strconv.Atoi(value)
			if err != nil {
				return domain.Manifest{}, fmt.Errorf("parse manifest version: %w", err)
			}
			manifest.Version = version
		case "name":
			manifest.Name = value
		case "stream_layout":
			layout, err := domain.ParseStreamLayout(value)
			if err != nil {
				return domain.Manifest{}, fmt.Errorf("parse manifest stream_layout: %w", err)
			}
			manifest.StreamLayout = layout
		case "history_mode":
			mode, err := domain.ParseHistoryMode(value)
			if err != nil {
				return domain.Manifest{}, fmt.Errorf("parse manifest history_mode: %w", err)
			}
			manifest.HistoryMode = mode
		case "created_at":
			if value == "" {
				continue
			}
			parsed, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return domain.Manifest{}, fmt.Errorf("parse manifest created_at: %w", err)
			}
			manifest.CreatedAt = parsed.UTC()
		}
	}

	return manifest.WithDefaults(), nil
}

func LoadManifest(path string) (domain.Manifest, error) {
	manifestPath := filepath.Join(path, "db.yaml")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.Manifest{Version: 1}.WithDefaults(), nil
		}
		return domain.Manifest{}, fmt.Errorf("read manifest: %w", err)
	}
	manifest, err := parseManifest(data)
	if err != nil {
		return domain.Manifest{}, err
	}
	return manifest.WithDefaults(), nil
}
