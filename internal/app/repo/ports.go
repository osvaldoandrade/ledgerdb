package repo

import (
	"context"
	"time"

	"github.com/codecompany/ledgerdb/internal/domain"
)

type Store interface {
	Init(ctx context.Context, path string) error
	WriteManifest(ctx context.Context, path string, manifest domain.Manifest) error
	SetRemote(ctx context.Context, path, name, url string) error
}

type StatusStore interface {
	LoadStatus(ctx context.Context, path string) (domain.RepoStatus, error)
}

type Cloner interface {
	Clone(ctx context.Context, url, path string) error
}

type Clock interface {
	Now() time.Time
}
