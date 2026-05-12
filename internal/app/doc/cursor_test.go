package doc

import (
	"errors"
	"strings"
	"testing"
)

func TestEncodeDecodeLogCursorRoundTrip(t *testing.T) {
	entry := LogEntry{TxHash: "abc123", Timestamp: 42}
	token := encodeLogCursor(entry)
	if token == "" {
		t.Fatal("expected non-empty cursor token")
	}

	decoded, err := decodeLogCursor(token)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.TxHash != entry.TxHash || decoded.Timestamp != entry.Timestamp {
		t.Fatalf("round-trip mismatch: %+v", decoded)
	}
	if decoded.Version != LogCursorVersion {
		t.Fatalf("expected version %d, got %d", LogCursorVersion, decoded.Version)
	}
}

func TestDecodeLogCursorRejectsGarbage(t *testing.T) {
	if _, err := decodeLogCursor("!!!not-base64!!!"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
}

func TestDecodeLogCursorRejectsWrongVersion(t *testing.T) {
	// {"v":99,"ts":1,"h":"x"} base64url'd
	const token = "eyJ2Ijo5OSwidHMiOjEsImgiOiJ4In0"
	_, err := decodeLogCursor(token)
	if !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
	if !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("expected version error, got %v", err)
	}
}

func TestDecodeLogCursorRejectsEmptyHash(t *testing.T) {
	// {"v":1,"ts":1,"h":""} base64url'd
	const token = "eyJ2IjoxLCJ0cyI6MSwiaCI6IiJ9"
	if _, err := decodeLogCursor(token); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expected ErrInvalidCursor, got %v", err)
	}
}
