package ident

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

type ULIDGenerator struct {
	entropy *ulid.MonotonicEntropy
}

func NewULIDGenerator() *ULIDGenerator {
	return &ULIDGenerator{entropy: ulid.Monotonic(rand.Reader, 0)}
}

func (g *ULIDGenerator) NewID() (string, error) {
	id, err := ulid.New(ulid.Timestamp(time.Now().UTC()), g.entropy)
	if err != nil {
		return "", fmt.Errorf("generate ulid: %w", err)
	}
	return id.String(), nil
}
