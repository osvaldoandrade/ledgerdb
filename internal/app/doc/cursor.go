package doc

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// LogCursorVersion is the current cursor schema version. The cursor format is
// versioned so callers can stay backward-compatible if the encoding evolves.
const LogCursorVersion = 1

// ErrInvalidCursor is returned when a supplied cursor cannot be decoded or is
// for an unsupported version.
var ErrInvalidCursor = errors.New("invalid cursor")

// logCursor is the in-memory representation of a doc-log cursor. Entries are
// emitted head-first (newest -> oldest), so callers anchor on the last entry
// they have seen.
type logCursor struct {
	Version   int    `json:"v"`
	Timestamp int64  `json:"ts"`
	TxHash    string `json:"h"`
}

// encodeLogCursor renders a cursor for the given entry as opaque base64(JSON).
func encodeLogCursor(entry LogEntry) string {
	payload := logCursor{
		Version:   LogCursorVersion,
		Timestamp: entry.Timestamp,
		TxHash:    entry.TxHash,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		// Marshaling a struct with primitive fields cannot fail in practice.
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// decodeLogCursor parses an opaque cursor produced by encodeLogCursor.
func decodeLogCursor(token string) (logCursor, error) {
	if token == "" {
		return logCursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return logCursor{}, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	var c logCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return logCursor{}, fmt.Errorf("%w: %v", ErrInvalidCursor, err)
	}
	if c.Version != LogCursorVersion {
		return logCursor{}, fmt.Errorf("%w: unsupported version %d", ErrInvalidCursor, c.Version)
	}
	if c.TxHash == "" {
		return logCursor{}, fmt.Errorf("%w: missing tx hash", ErrInvalidCursor)
	}
	return c, nil
}
