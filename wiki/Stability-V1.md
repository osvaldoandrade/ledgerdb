# Stability: v1.0 Contract

LedgerDB is currently in the **0.2.x** series. Some surfaces are already widely used in tests, demos, and downstream prototypes; others are expected to keep evolving. This page is the explicit, contract-style statement of **what v1.0 will freeze** and **what remains free to change** after v1.0. It is the document that every change touching a frozen surface must be reconciled against.

The page is intentionally narrow. Anything not listed here as frozen should be assumed mutable, and the burden of proof for "but I expected this to be stable" sits with the caller, not with the project.

## What v1.0 freezes

The following surfaces are the LedgerDB **stable contract**. Once v1.0 ships, breaking changes to any of these require either a new major version, or a documented forward-compatible extension (see the extension points below). Pre-v1.0 they still go through the deprecation flow wherever feasible — see [Deprecation Policy](Stability-Deprecation).

### TxV3 wire format

- **Location**: `internal/infra/txv3/`.
- **Scope**: the Protobuf message definitions, field numbers, and the canonical byte-level serialisation used to compute transaction hashes (see [TxV3 Format](IO-TxV3-Format)).
- **Why**: existing ledgers must remain readable and verifiable forever. Changing the wire format would invalidate every hash chain in existence.

After v1.0, the on-disk bytes of every TxV3 blob produced by v1.0 must remain valid for all subsequent v1.x readers. The verifier in `internal/app/integrity/verify_service.go` is the last-line check that this property holds.

### CLI command surface

- **Location**: `internal/cli/`, surfaced through `cmd/ledgerdb`.
- **Scope**: the names and semantics of top-level commands (`init`, `collection`, `doc`, `index`, `integrity`, `inspect`, `maintenance`), their subcommands, the names of their flags, and the meaning of their exit codes.

What v1.0 guarantees:

- A command or flag will not be **renamed** or **removed** without going through the [Deprecation Policy](Stability-Deprecation).
- Flag *additions* are allowed and considered non-breaking, provided defaults preserve existing behaviour.
- Output formats marked `--format json` (and the structured payloads behind `--json`) are part of the contract; the default human-readable text output is **not** — it may be reformatted for clarity at any time.

The exit-code taxonomy follows the mapping in `internal/cli/errors.go`: `1` internal, `2` validation, `3` not found, `4` conflict. Scripts that branch on those codes are reading a contracted surface.

### Public Go SDK types

- **Location**: `pkg/ledgerdbsdk/`.
- **Scope**: every exported (`PascalCase`) type, function, method, and constant in `pkg/ledgerdbsdk/...`. This is the surface that downstream Go consumers import.

What v1.0 guarantees:

- Exported signatures are stable — adding parameters, removing methods, or changing return types requires a deprecation cycle.
- New exported APIs may be added freely.
- Behavioural contracts documented in the package GoDoc are honoured; silent behaviour drift is treated as a bug.

Anything that the SDK re-exports from `internal/` is considered part of the public surface for stability purposes. The page [Go SDK](SDK-Go-SDK) walks the current shape.

### Manifest schema

- **Scope**: the on-disk manifest produced by `ledgerdb init` and read on every load — collection definitions, layout descriptors, history-mode selection, and the manifest's own versioning field.
- **Why**: the manifest is the entry point that maps a Git repo to a LedgerDB instance. Existing repos must continue to open with the latest v1.x CLI.

Schema *extensions* are allowed (new optional fields, new layout values behind a feature flag); removals and renames are not. The manifest's version field carries its own additive-vs-breaking semantics; see the extension points below.

## What remains unstable after v1.0

The following are **explicitly not part of the stability contract** and may change in any release, including patch releases:

- **`internal/*` packages.** By Go convention these are private. Anything importing them does so at its own risk and should be migrated onto the public SDK in `pkg/ledgerdbsdk/`.
- **Sidecar SQLite schema.** The per-collection tables, indexes, and metadata rows produced by `ledgerdb index sync` / `ledgerdb index watch`. The sidecar is a *projection* of the canonical Git state and is fully rebuildable; the project reserves the right to add columns, change types, or rebuild from scratch when it improves query performance. The schema documented at [SQLite Schema](IO-SQLite-Schema) is best-effort, not contracted.
- **Benchmark harness.** Bench commands, output schemas, and harness flags exist to drive performance work; they are not a stable interface.
- **Local web console.** When it ships under Epic [#3](https://github.com/osvaldoandrade/ledgerdb/issues/3), the HTML/JS, the URL routes, and any private JSON it returns are not stable.
- **Logging output.** Both human-readable text and the precise structure of slog JSON events. Log *level* names are stable; log *messages* are not. The field-name conventions on [Observability Logging](Observability-Logging) are best-effort.
- **Error message strings.** Error *types* and *codes* are stable, but the human-readable strings attached to them may improve over time. Scripts that match on error strings are reading an unstable surface; scripts that branch on the exit-code taxonomy or on `errors.Is` against the SDK's sentinel set (`pkg/ledgerdbsdk/errors.go`) are reading a stable one.

## Forward-compatibility hooks

The v1.0 frozen surfaces have been designed with extension points so the project can grow without breaking.

### TxV3 versioning field

The TxV3 message carries a **version** field. v1.0 readers must:

1. Accept any TxV3 blob whose version is equal to the writer's version.
2. Reject blobs whose major version is higher than they understand, with a clear error.
3. Treat minor-version increments as additive: new fields may appear and must be preserved on round-trip.

This lets the format evolve additively post-v1.0 without breaking existing chains. A true wire-format break would require v2.0.

### Manifest version

The manifest carries its own version field with the same semantics: minor bumps for additive changes, major bumps for breaking ones. The default sharded layout introduced in manifest version 2 (`internal/domain/manifest.go:5`) is the prototype for how a layout change lands: the older layout remains readable and writable; the newer layout becomes the default for fresh repos.

### SDK feature flags

The Go SDK config supports forward-compatible booleans and option structs. New behaviours land behind a flag that defaults to the legacy behaviour, then flip the default in a later minor release after a deprecation cycle. The `Index` substruct on `ledgerdbsdk.Config` is the canonical example: every flag is optional, with documented defaults applied at `normalizeConfig` time.

## Pre-v1.0 caveat

LedgerDB is not yet at v1.0. Until v1.0 ships:

- **Minor releases (0.x → 0.(x+1)) may include breaking changes.** When they do, the release notes call them out explicitly and link migration guidance.
- **Patch releases (0.x.y → 0.x.(y+1)) must not break compatibility.**
- The [Deprecation Policy](Stability-Deprecation) still applies *whenever feasible*; the project will phase removals through warnings when it can, but reserves the right to ship larger pre-v1.0 breakage on a shorter cycle if it materially improves the v1.0 contract.

Once v1.0 is tagged, LedgerDB adopts **strict semantic versioning**:

- **MAJOR** for any change to a frozen surface listed above.
- **MINOR** for backward-compatible additions.
- **PATCH** for bug fixes that do not change behaviour for compliant callers.

## Open questions before v1.0

These are surfaced rather than hidden. They must be resolved in writing — captured as wiki pages under the relevant section per [GOVERNANCE.md](https://github.com/osvaldoandrade/ledgerdb/blob/main/GOVERNANCE.md) §3 — before v1.0 can be tagged:

1. Final shape of the conflict-resolution API (Epic [#88](https://github.com/osvaldoandrade/ledgerdb/issues/88)).
2. Stable error code taxonomy across CLI and SDK.
3. Long-term GPG key rotation policy for signed releases.
4. Whether the sidecar SQLite schema becomes a stable *read-only* contract for downstream BI tooling, or remains a pure projection.

Each open question will be answered with a wiki page under the relevant section; the answer becomes part of the v1.0 freeze when it merges.

## See also

- [Deprecation Policy](Stability-Deprecation) — the warning-then-removal flow that backs every change to a frozen surface.
- [Go SDK](SDK-Go-SDK) — the current shape of the public Go surface.
- [CLI Reference](SDK-CLI-Reference) — the current shape of the public CLI surface.
- [TxV3 Format](IO-TxV3-Format) — the on-disk format that v1.0 freezes byte-for-byte.
- [SQLite Schema](IO-SQLite-Schema) — the sidecar projection that v1.0 explicitly does **not** freeze.
- `internal/infra/txv3/` — the protobuf definitions and encoder.
- `internal/domain/manifest.go` — the manifest type and its version field.
- `pkg/ledgerdbsdk/` — the public Go SDK surface.
