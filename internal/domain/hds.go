package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
)

const (
	DocumentsRoot = "documents"
	StateRoot     = "state"
	HDSSeparator  = "/"
)

func HDSHash(collection, key string) string {
	payload := collection + HDSSeparator + key
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func StreamPath(layout StreamLayout, collection, key string) string {
	layout = NormalizeStreamLayout(layout)
	hash := HDSHash(collection, key)
	switch layout {
	case StreamLayoutSharded:
		return filepath.Join(DocumentsRoot, collection, hash[0:2], hash[2:4], "DOC_"+hash)
	default:
		return filepath.Join(DocumentsRoot, collection, "DOC_"+hash)
	}
}

func HDSPath(collection, key string) string {
	return StreamPath(StreamLayoutFlat, collection, key)
}

func StatePath(layout StreamLayout, collection, key string) string {
	layout = NormalizeStreamLayout(layout)
	hash := HDSHash(collection, key)
	switch layout {
	case StreamLayoutSharded:
		return filepath.Join(StateRoot, collection, hash[0:2], hash[2:4], "DOC_"+hash)
	default:
		return filepath.Join(StateRoot, collection, "DOC_"+hash)
	}
}
