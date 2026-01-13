package gitrepo

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
)

const (
	envGitUser    = "LEDGERDB_GIT_USERNAME"
	envGitToken   = "LEDGERDB_GIT_TOKEN"
	envGHToken    = "GITHUB_TOKEN"
	envGHCLIToken = "GH_TOKEN"
	defaultUser   = "x-access-token"
)

func authForURL(rawURL string) (transport.AuthMethod, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, nil
	}

	ep, err := transport.NewEndpoint(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse remote URL: %w", err)
	}

	switch ep.Protocol {
	case "http", "https":
		token := firstNonEmpty(os.Getenv(envGitToken), os.Getenv(envGHToken), os.Getenv(envGHCLIToken))
		if token == "" {
			return nil, nil
		}
		user := strings.TrimSpace(os.Getenv(envGitUser))
		if user == "" {
			user = defaultUser
		}
		return &http.BasicAuth{
			Username: user,
			Password: token,
		}, nil
	default:
		return nil, nil
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
