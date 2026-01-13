package hash

import (
	"crypto/sha256"
	"encoding/hex"
)

type SHA256 struct{}

func (SHA256) SumHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
