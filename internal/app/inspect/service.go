package inspect

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/codecompany/ledgerdb/internal/app/paths"
)

type Service struct {
	reader  BlobReader
	decoder Decoder
	hasher  Hasher
}

func NewService(reader BlobReader, decoder Decoder, hasher Hasher) *Service {
	return &Service{
		reader:  reader,
		decoder: decoder,
		hasher:  hasher,
	}
}

func (s *Service) InspectBlob(ctx context.Context, repoPath, objectHash string) (BlobResult, error) {
	objectHash = strings.TrimSpace(objectHash)
	if objectHash == "" {
		return BlobResult{}, ErrHashRequired
	}
	if !isValidGitHash(objectHash) {
		return BlobResult{}, ErrInvalidHash
	}

	absRepoPath, err := paths.NormalizeRepoPath(repoPath)
	if err != nil {
		return BlobResult{}, err
	}

	data, err := s.reader.ReadBlob(ctx, absRepoPath, objectHash)
	if err != nil {
		return BlobResult{}, err
	}

	tx, err := s.decoder.Decode(data)
	if err != nil {
		return BlobResult{}, fmt.Errorf("decode tx: %w", err)
	}

	return BlobResult{
		ObjectHash: objectHash,
		TxHash:     s.hasher.SumHex(data),
		Tx:         tx,
	}, nil
}

func isValidGitHash(value string) bool {
	if len(value) != 40 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
