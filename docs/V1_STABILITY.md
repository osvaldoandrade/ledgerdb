# LedgerDB v1.0 Stability Plan

LedgerDB is currently in the **0.2.x** series. Some of the surfaces are
already widely used in tests, demos, and downstream prototypes; others are
expected to keep evolving. This document is the explicit, contract-style
statement of **what v1.0 will freeze** and **what remains free to change**
after v1.0.

It is intentionally narrow: anything not listed here as frozen should be
assumed mutable.

---

## 1. What v1.0 freezes

The following surfaces are the LedgerDB **stable contract**. Once v1.0
ships, breaking changes to any of these require either (a) a new major
version, or (b) a documented forward-compatible extension — see section 3.

### 1.1 TxV3 wire format

- **Location**: `internal/infra/txv3/`.
- **Scope**: the Protobuf message definitions, field numbers, and the
  canonical byte-level serialisation used to compute transaction hashes.
- **Why**: existing ledgers must remain readable and verifiable forever.
  Changing the wire format would invalidate every hash chain in existence.

After v1.0, the on-disk bytes of every TxV3 blob produced by v1.0 must
remain valid for all subsequent v1.x readers.

### 1.2 CLI command surface

- **Location**: `internal/cli/`, surfaced through `cmd/ledgerdb`.
- **Scope**: the names and semantics of top-level commands (`init`,
  `collection`, `doc`, `index`, `integrity`, `inspect`, `maintenance`),
  their subcommands, the names of their flags, and the meaning of their
  exit codes.

What v1.0 guarantees:

- A command or flag will not be **renamed** or **removed** without going
  through the deprecation policy in `docs/DEPRECATION.md`.
- Flag *additions* are allowed and considered non-breaking, provided
  defaults preserve existing behaviour.
- Output formats marked `--format json` are part of the contract; the
  default human-readable text output is **not** — it may be reformatted
  for clarity at any time.

### 1.3 Public Go SDK types

- **Location**: `pkg/ledgerdbsdk/`.
- **Scope**: every exported (`PascalCase`) type, function, method, and
  constant in `pkg/ledgerdbsdk/...`. This is the surface that downstream
  Go consumers import.

What v1.0 guarantees:

- Exported signatures are stable — adding parameters, removing methods,
  or changing return types requires a deprecation cycle.
- New exported APIs may be added freely.
- Behavioural contracts documented in the package GoDoc are honoured;
  silent behaviour drift is treated as a bug.

Anything that the SDK *re-exports* from `internal/` is considered part of
the public surface for stability purposes.

### 1.4 Manifest schema

- **Scope**: the on-disk manifest produced by `ledgerdb init` and read on
  every load — collection definitions, layout descriptors, history-mode
  selection, and the manifest's own versioning field.
- **Why**: the manifest is the entry point that maps a Git repo to a
  LedgerDB instance. Existing repos must continue to open with the latest
  v1.x CLI.

Schema *extensions* are allowed (new optional fields, new layout values
behind a feature flag); removals and renames are not.

---

## 2. What remains unstable after v1.0

The following are **explicitly not part of the stability contract** and
may change in any release, including patch releases:

- **`internal/*` packages** — by Go convention these are private. Anything
  importing them does so at its own risk and should be migrated onto the
  public SDK.
- **Sidecar SQLite schema** — the per-collection tables, indexes, and
  metadata rows produced by `ledgerdb index sync` / `index watch`. The
  sidecar is a *projection* of the canonical Git state and is fully
  rebuildable; we reserve the right to add columns, change types, or
  rebuild from scratch when it improves query performance.
- **Benchmark harness** — bench commands, output schemas, and harness
  flags exist to drive performance work; they are not a stable interface.
- **Local web console** (Epic #3, when shipped) — the HTML/JS, the URL
  routes, and any private JSON it returns are not stable.
- **Logging output** — both human-readable text and the precise structure
  of slog JSON events. Log *level* names are stable; log *messages* are
  not.
- **Error message strings** — error *types* and *codes* are stable, but
  the human-readable strings attached to them may improve over time.

---

## 3. Forward-compatibility hooks

We have intentionally designed the v1.0 frozen surfaces with extension
points so we can grow without breaking.

### 3.1 TxV3 versioning field

The TxV3 message carries a **version** field. v1.0 readers must:

1. Accept any TxV3 blob whose version is equal to the writer's version.
2. Reject blobs whose major version is higher than they understand, with
   a clear error.
3. Treat minor-version increments as additive: new fields may appear and
   must be preserved on round-trip.

This lets us evolve the format additively post-v1.0 without breaking
existing chains. A true wire-format break would require v2.0.

### 3.2 Manifest version

The manifest carries its own version field with the same semantics: minor
bumps for additive changes, major bumps for breaking ones.

### 3.3 SDK feature flags

The Go SDK config supports forward-compatible booleans and option structs.
New behaviours land behind a flag that defaults to the legacy behaviour,
then flip the default in a later minor release after a deprecation cycle.

---

## 4. Pre-v1.0 caveat

We are not yet at v1.0. Until v1.0 ships:

- **Minor releases (0.x → 0.(x+1)) may include breaking changes.** When
  they do, the release notes call them out explicitly and link migration
  guidance.
- **Patch releases (0.x.y → 0.x.(y+1)) must not break compatibility.**
- The deprecation policy in `docs/DEPRECATION.md` still applies *whenever
  feasible*; we will phase removals through warnings when we can, but we
  reserve the right to ship larger pre-v1.0 breakage on a shorter cycle
  if it materially improves the v1.0 contract.

Once v1.0 is tagged, LedgerDB adopts **strict semantic versioning**:

- **MAJOR** for any change to a frozen surface listed in section 1.
- **MINOR** for backward-compatible additions.
- **PATCH** for bug fixes that do not change behaviour for compliant
  callers.

---

## 5. Open questions before v1.0

These are intentionally surfaced rather than hidden — they must be
resolved (in writing, via RFCs under `docs/`) before v1.0 can be tagged:

1. Final shape of the conflict-resolution API (Epic #88).
2. Stable error code taxonomy across CLI and SDK.
3. Long-term GPG key rotation policy for signed releases.
4. Whether the sidecar SQLite schema becomes a stable *read-only* contract
   for downstream BI tooling, or remains a pure projection.

The answers will land in `docs/` as additional ADRs and will be referenced
from this file.
