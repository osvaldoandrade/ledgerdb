package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"
)

func TestHDSHashMatchesSpecSeparator(t *testing.T) {
	collection := "users"
	key := "user_123"
	payload := collection + "/" + key
	sum := sha256.Sum256([]byte(payload))
	expected := hex.EncodeToString(sum[:])

	if got := HDSHash(collection, key); got != expected {
		t.Fatalf("expected hash %q, got %q", expected, got)
	}
}

func TestHDSPathBuildsExpectedLayout(t *testing.T) {
	collection := "users"
	key := "user_123"
	hash := HDSHash(collection, key)
	expected := filepath.Join("documents", collection, "DOC_"+hash)

	if got := HDSPath(collection, key); got != expected {
		t.Fatalf("expected path %q, got %q", expected, got)
	}
}

func TestStreamPathShardedLayout(t *testing.T) {
	collection := "users"
	key := "user_123"
	hash := HDSHash(collection, key)
	expected := filepath.Join("documents", collection, hash[0:2], hash[2:4], "DOC_"+hash)

	if got := StreamPath(StreamLayoutSharded, collection, key); got != expected {
		t.Fatalf("expected path %q, got %q", expected, got)
	}
}
