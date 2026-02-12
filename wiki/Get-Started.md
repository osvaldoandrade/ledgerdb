# Get Started

This page is the shortest path to run LedgerDB locally and validate write, read, and index flows.

## Prerequisites

- Go 1.24+
- Git installed and available in PATH
- Local filesystem path for repository and SQLite index

## Build

```bash
make build
```

## Initialize Repository

```bash
ledgerdb init --name "LedgerDB" --repo ./ledgerdb.git --layout sharded --history-mode append
```

## Apply Schema and Write Data

```bash
ledgerdb collection apply tasks --schema ./schemas/task.json --indexes "status,assignee"
ledgerdb doc put tasks "task_0001" --payload '{"title":"Ship v1","status":"todo","priority":"high"}'
ledgerdb doc get tasks "task_0001"
```

## Build and Watch SQLite Index

```bash
ledgerdb index watch --db ./index.db --mode state --interval 1s --fast --batch-commits 200
```

## Validate Integrity

```bash
ledgerdb integrity verify --deep
```

## Next Pages

- [Execution Model and Consistency](Execution-Model-and-Consistency)
- [Querying and Indexing Strategy](Querying-and-Indexing-Strategy)
- [Use Cases](Use-Cases)
