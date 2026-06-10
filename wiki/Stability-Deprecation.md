# Stability: Deprecation Policy

LedgerDB is a database. Compatibility matters. This page defines how features are phased out so operators have predictable migration windows and nothing disappears on them without warning.

It applies to all surfaces covered by the [v1.0 Contract](Stability-V1) — namely CLI commands and flags, public Go SDK types, manifest fields, and the TxV3 wire format — and best-effort to surfaces marked explicitly unstable.

## Policy

When LedgerDB deprecates a feature:

1. The release that introduces the deprecation **adds the replacement first**. Users always have a forward path available before the old path starts warning.
2. Using the deprecated feature emits a **warning via `slog`** at `WARN` level on first use per process. The warning includes:
   - The name of the deprecated item.
   - The version it will be removed in.
   - The replacement, with a one-line example or a doc link.
3. The deprecation is documented in the release notes for that version under a clearly marked **Deprecations** heading.
4. The warning **persists for at least one full minor release** before removal.
5. Removal lands in a subsequent minor release, again called out in the release notes under **Removals**, with a link back to the deprecation announcement.

The minimum lifecycle is therefore three minor releases: introduce replacement and deprecate (N), warn (N+1), remove (N+2). The actual window is often longer for surfaces that have many downstream consumers.

After v1.0, removal of any frozen surface requires a major version bump on top of the warning window described above. See [v1.0 Contract](Stability-V1) for the list of frozen surfaces.

## Example timeline

A concrete worked example for a hypothetical flag rename:

| Release | Action                                                                                                                                                    |
|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|
| **0.5** | Add the new flag `--shard-bits`. The old flag `--shard-depth` continues to work but emits a deprecation warning the first time it is used.                |
| **0.6** | Warning persists. `--shard-depth` still works. No-op release for this flag (it may receive bug fixes in lockstep with `--shard-bits`).                    |
| **0.7** | `--shard-depth` is removed. Using it now fails with a clear error that names the replacement and links to the deprecation entry in the 0.5 release notes. |

The same pattern applies to SDK methods, manifest fields, and command names. CLI commands that are being renamed will accept the old name as a hidden alias during the warning window so existing scripts do not break.

## Suppressing warnings during migration

Deprecation warnings exist to be acted on, not to be permanent log noise. While migrating, callers can suppress them by setting:

```
LEDGERDB_SUPPRESS_DEPRECATION_WARNINGS=1
```

When this variable is truthy (`1`, `true`, `yes`), `slog` deprecation warnings are downgraded to `DEBUG` and will not appear at the default log level. The environment variable is recognised by both the CLI and the public Go SDK.

This is intended as a **temporary** measure for noisy migrations — for example, a CI pipeline that hits a deprecated flag dozens of times per run. Leaving it set permanently is strongly discouraged: you will not see the next round of deprecations, and you will be caught out at the removal release.

## What we will not deprecate

Some changes do not go through this policy because they are not user-visible breaks:

- **Bug fixes** that bring behaviour into line with documented intent. The project treats the docs as the source of truth; if code was diverging from them, fixing it is a fix, not a deprecation.
- **Internal package changes** (`internal/*`). These are not part of the stability contract and may move without warning, though the project tries to minimise churn that would affect downstream forks.
- **Log message wording** and human-readable error strings. Field-name conventions on [Observability Logging](Observability-Logging) are best-effort; the exact `msg` strings are not.

Conversely, things that *do* go through the policy even when they look small:

- Adding a required flag to a previously optional command.
- Changing the default value of an existing flag in a way that alters behaviour for existing callers.
- Removing a manifest field, even if it is currently optional.

The discriminator is whether a user who only read the prior release notes would be surprised by the new behaviour. If yes, deprecation. If no, ship it.

## How to report unannounced breakage

If a release breaks you without a corresponding deprecation in a prior release, file a bug using the **bug** issue template at [github.com/osvaldoandrade/ledgerdb/issues](https://github.com/osvaldoandrade/ledgerdb/issues). Include the prior version you were on, the new version that broke you, and the failing command or SDK call. The project treats undeclared breakage as a high-priority bug.

## See also

- [v1.0 Contract](Stability-V1) — the list of surfaces this policy protects.
- [CLI Reference](SDK-CLI-Reference) — the canonical CLI surface.
- [Go SDK](SDK-Go-SDK) — the canonical Go SDK surface; `pkg/ledgerdbsdk/errors.go` lists the stable sentinel error set.
- [GOVERNANCE.md](https://github.com/osvaldoandrade/ledgerdb/blob/main/GOVERNANCE.md) — the RFC process for changes that warrant a deprecation cycle in the first place.
