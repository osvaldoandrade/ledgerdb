# Troubleshooting

## Write Rejected Due to CAS Conflict

Symptoms:

- `doc put` fails under concurrent writes.

Checks:

1. Confirm another writer advanced `refs/heads/main`.
2. Retry with backoff in client flow.
3. Inspect transaction log for conflicting parent state.

## Index Lag or Missing Query Results

Symptoms:

- Recent writes do not appear in SQLite results.

Checks:

1. Verify `index watch` is running with expected interval/mode.
2. Ensure schema/index definitions include queried fields.
3. Run one-shot `index sync` to rescan current state.

## Integrity Verification Failure

Symptoms:

- `integrity verify` reports chain mismatch.

Checks:

1. Validate repository object availability and refs.
2. Recompute from known good head or snapshot.
3. Audit recent writes for non-deterministic payload changes.

## Sync/Replication Issues

Symptoms:

- Push/pull fails or peers diverge.

Checks:

1. Verify remote Git connectivity and auth.
2. Inspect branch divergence and merge strategy.
3. Run status and inspect commands before replay/revert.
