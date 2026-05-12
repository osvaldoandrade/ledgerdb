# Contributing to LedgerDB

Thank you for your interest in contributing to LedgerDB. This document explains
how to set up a development environment, the conventions we follow, and how to
get your changes merged.

LedgerDB is a Git-native immutable document database written in Go. The core
storage layer is intentionally small; most of the complexity lives in the CLI
and SDKs. Contributions of any size are welcome, from typo fixes to new
storage features.

---

## 1. Development environment

### 1.1 Toolchain

LedgerDB pins its Go toolchain in `go.mod`. At the time of writing this is
**Go 1.25**. Always use the version declared there — do not downgrade. If you
use a version manager (`gvm`, `asdf`, `mise`), point it at the `go` directive
in `go.mod`.

You will also need:

- **Git 2.40+** (the storage engine talks to a real Git repo on disk).
- **GNU Make** (for the convenience targets in the `Makefile`).
- **SQLite 3** (libsqlite is bundled by the CGO build; only required at
  runtime for the sidecar index).
- A C toolchain (`gcc` or `clang`) if you build the shared-library target
  via `make build-core-shared`.

### 1.2 Building

```bash
# CLI binary in ./bin/ledgerdb
make build

# Optional: build the C-shared core library used by foreign SDKs
make build-core-shared
```

`make install` will copy the CLI into `$PREFIX/bin` (defaults to the first
writable directory on `PATH`). Override with `make install PREFIX=/usr/local`.

### 1.3 Running the test suite

```bash
# Unit + package tests
go test ./...

# Race-checked run (slower; required before sending PRs touching concurrency)
go test -race ./...

# Static analysis and lint
go vet ./...
golangci-lint run
```

Integration scripts and end-to-end fixtures live under `tests/`. They drive
the CLI against ephemeral bare repositories and assert on the resulting Git
state. Run them with the helpers documented in `tests/README.md` (if present)
or by invoking the individual shell scripts directly.

When you add a new feature, please include both:

1. A focused unit test next to the package under change.
2. Where the surface is user-visible (CLI flag, SDK method, on-disk format),
   an integration test under `tests/` that exercises the full path.

---

## 2. Project layout

A short tour of the directories you are most likely to touch:

- `cmd/ledgerdb/main.go` — CLI entry point.
- `internal/cli/` — Cobra command definitions and flag wiring.
- `internal/app/` — application services orchestrating use cases.
- `internal/domain/` — pure domain types (documents, transactions, refs).
- `internal/infra/` — Git, SQLite, filesystem, and TxV3 protobuf adapters.
- `pkg/ledgerdbsdk/` — public Go SDK; treat this as the **stable** surface.
- `docs/` — design documents, ordered `01..09`, plus governance docs.

Anything under `internal/` is private and may change without notice. Anything
under `pkg/` follows the stability policy in `docs/V1_STABILITY.md`.

---

## 3. Pull request guidelines

### 3.1 Keep PRs small and focused

We strongly prefer **small, single-purpose PRs**. A good rule of thumb: if you
cannot summarise the change in one sentence without the word "and", split it.

- One logical change per PR.
- Refactors that move code around should land separately from behaviour
  changes, so reviewers can verify each diff is a pure rename or a pure
  rewrite.
- Drive-by formatting fixes should be in their own commit (or PR) so the
  substantive diff stays reviewable.

### 3.2 Link an issue

Every PR should link the issue it resolves using GitHub's auto-close syntax:

```
Closes #123
```

If a PR closes multiple issues, list each one (`Closes #123, closes #124`).
For work-in-progress that does not yet close an issue, use `Refs #N`.

### 3.3 Description

Use the PR template (`.github/PULL_REQUEST_TEMPLATE.md`). At minimum your
description must contain:

- **Summary** — 1–3 bullets explaining the *why*.
- **Test plan** — the commands you ran, plus any manual verification.
- **Linked issue** — `Closes #N`.
- **Checklist** — confirm tests pass and docs are updated.

### 3.4 Review and merge

- All PRs are squash-merged so the main branch keeps a linear history.
- The squash commit message should still follow conventional commits (see
  below); GitHub will use the PR title by default, so write the title
  accordingly.
- Maintainers may push small follow-up edits before merging; if you would
  rather they not, say so in the PR description.

---

## 4. Commit conventions

LedgerDB uses [Conventional Commits](https://www.conventionalcommits.org/).
The accepted **types** are:

| Type       | When to use it                                                            |
|------------|---------------------------------------------------------------------------|
| `feat`     | A user-visible feature (new flag, new SDK method, new command).           |
| `fix`      | A bug fix that changes runtime behaviour.                                 |
| `docs`     | Documentation-only changes, including this file.                          |
| `test`     | Adding or refactoring tests with no production code change.               |
| `perf`     | Performance improvements with no behavioural change.                      |
| `refactor` | Internal restructuring with no behavioural or API change.                 |
| `chore`    | Build, dependency, or repo-maintenance tasks.                             |
| `ci`       | Changes to GitHub Actions workflows or other CI plumbing.                 |

Format:

```
<type>(<optional-scope>): <imperative summary, ~72 chars>

<body explaining motivation, context, and any non-obvious choices>

Closes #N
```

Breaking changes must include a `BREAKING CHANGE:` footer describing the
migration. Before v1.0 we sometimes ship breakage in minor releases; see
`docs/V1_STABILITY.md` for the full policy.

Examples:

```
feat(cli): add --jitter flag to index watch
fix(infra/git): retry CAS on stale ref instead of erroring
docs: clarify state-mode indexing in README
```

---

## 5. Branch naming

Work branches should be prefixed by their kind:

- `feature/<short-slug>` — new functionality.
- `fix/<short-slug>` — bug fixes.
- `docs/<short-slug>` — docs-only changes.
- `chore/<short-slug>` — build/CI/dependency work.

Keep slugs lowercase with hyphens, and short (≤ 5 words). Example:
`feature/index-watch-jitter`.

The `main` branch is protected. Pushes go through pull requests.

---

## 6. Sign-off (DCO)

By contributing to LedgerDB you certify the
[Developer Certificate of Origin](https://developercertificate.org/). Add a
`Signed-off-by` line to each commit using `git commit -s`. The trailer must
match the author and use a real name:

```
Signed-off-by: Jane Doe <jane@example.com>
```

PRs missing DCO sign-off will be flagged by CI and cannot be merged until the
trailer is added (rebase + `git commit --amend -s`, or
`git rebase --signoff main`).

---

## 7. Getting help

- Open a GitHub Discussion for questions, design conversations, or "is this
  the right approach?" sanity checks.
- File an issue with the appropriate template for bugs, feature requests,
  epics, or scoped tasks.
- For security issues, **do not** open a public issue — follow
  `SECURITY.md`.

We aim to acknowledge new issues and PRs within a few business days. Thank
you for helping make LedgerDB better.
