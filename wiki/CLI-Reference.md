# CLI Reference

LedgerDB CLI is the primary operator interface for repository, document, index, integrity, and maintenance operations.

## Repository Commands

- `ledgerdb init`
- `ledgerdb clone`
- `ledgerdb status`

## Document Commands

- `ledgerdb doc put`
- `ledgerdb doc get`
- `ledgerdb doc patch`
- `ledgerdb doc delete`
- `ledgerdb doc log`
- `ledgerdb doc revert`

## Collection Commands

- `ledgerdb collection apply`

## Index Commands

- `ledgerdb index sync`
- `ledgerdb index watch`

## Integrity and Maintenance

- `ledgerdb integrity verify`
- `ledgerdb maintenance gc`
- `ledgerdb maintenance snapshot`

## Global Operational Flags

- `--sync=false` for offline write mode
- `--sign` and `--sign-key` for commit signing
- `--log-level` and `--log-format` for observability
