# SDK REPL And Query Explain

Two of the commands on [SDK CLI Reference](SDK-CLI-Reference) deserve their own page because they are the operator tools that fall outside the usual "one-shot subcommand" shape: `ledgerdb repl` is the interactive shell, and `ledgerdb query explain` is the SQLite-plan inspector. Both were added in commit `a7b784c` and both target the same audience — somebody who has a repository in front of them and is trying to figure out what is in it, what a query will cost, or how a sequence of writes will compose. This page walks through each in detail, citing the implementing files, and ends with a small set of patterns that combine the two.

## The REPL

The REPL is implemented in [`internal/cli/cmd_repl.go`](../tree/main/internal/cli/cmd_repl.go). It is short — a hair over 200 lines — because most of what it does is delegate back to the cobra root command. The structure is: read a line, tokenise it into argv, prepend the persistent flags the REPL inherited, dispatch through a fresh root command, render any error through the standard CLI error path. The loop is conceptually `while line := scanner.Scan() { dispatchReplCommand(line) }` and most of the file is the tokeniser and the inheritance plumbing.

### Invocation

```
ledgerdb repl              # interactive against stdin
ledgerdb repl --script <path>   # batch read from a file, no prompt, exit at EOF
```

In interactive mode the REPL prints `LedgerDB REPL. Type \h for help, \q or exit to quit.` once and then loops with the prompt `ledgerdb> ` (or `<repo-path>> ` if `--repo` is not `.`). In script mode it suppresses the banner and the prompt, reads each non-empty non-comment line, and dispatches it the same way. Script mode is the right shape for migrations driven by a versioned set of commands or for repeatable demos.

The REPL inherits its parent's persistent flags. Anything passed to `ledgerdb` on the outer invocation — `--repo`, `--json`, `--sign`, `--log-level` — is captured at the REPL's entry and re-prepended to every inner command unless the user already supplied that flag. The implementation lives in `replInheritedArgs`; the helper `replArgsContainFlag` is the textual check that prevents double-prepending. Because the dispatch builds a fresh `newRootCmd()` each iteration, the inner commands see a clean cobra state — no leftover flags from the previous line, no cobra "command already executed" complaints.

### What you can type at the prompt

Anything that is a valid `ledgerdb` command. There is no separate REPL language. `doc put users u1 --payload '{"name":"alice"}'` works at the prompt the same way it works in a shell. So does `index sync --db ./index.db --mode state --once`. So does `stats`. The tokeniser supports double-quoted strings (with `\"` and `\\` escapes), single-quoted strings (no escape handling, suitable for JSON payloads with embedded double quotes), and unquoted bare tokens. The implementation is `splitReplLine` and the rules are deliberately simpler than `bash` — there is no command substitution, no variable expansion, no piping.

The REPL adds three built-ins:

| Input | Effect |
|---|---|
| `\h` or `help` | Print the short help text (`printReplHelp`). |
| `\q`, `exit`, `quit` | Leave the REPL with exit code 0. |
| empty line | Skip; re-prompt. |

There is no command history file, no readline binding, no tab completion. The REPL uses `bufio.NewScanner` with a 1 MiB buffer (`scanner.Buffer(make([]byte, 64*1024), 1024*1024)`) which is enough for any single-line `doc put --payload <json>` but not for multi-line input. Multi-line input is therefore handled by the shell or the editor that feeds the REPL — either pipe a multi-line `here-doc` in, or use `--script` against a file.

Errors from inner commands are normalised through `NormalizeError` and written through `writeCLIError` (see `internal/cli/errors.go`) just as they would be at the top level. The REPL does not exit on error; it logs the error to stderr and re-prompts. This matters for script-mode runs where a single failure should not abort a batch unless the operator wants it to — wrap the script in `set -e`-equivalent tooling if abort-on-error is required.

### Interactive workflow

A typical session is investigative. An operator who has just been handed a repository with an unfamiliar shape does roughly this:

```
$ ledgerdb --repo ./incoming.git repl
LedgerDB REPL. Type \h for help, \q or exit to quit.
incoming.git> status
Path: incoming.git
Bare: false
Head: 9c8e1ab4...
Manifest: payments (v3)
Stream Layout: sharded
History Mode: append
incoming.git> stats
Repo
  Path: incoming.git
  Layout: sharded
  History: append
  ...
Collections
  Name                     Docs       Avg Chain    Max Chain    Last Tx
  invoices                 12483      4.21         147          2025-09-12T11:04:32Z
  customers                1180       2.05         9            2025-09-12T11:01:18Z
incoming.git> doc log invoices inv_0001 --limit 5
...
incoming.git> \q
```

The REPL is not a replacement for `ledgerdb` in a shell pipeline. It is a way to amortise the `git fetch` cost when `--sync=true` over many sequential commands, and a way to keep a single repository context across many lookups without retyping `--repo`. Both matter when the repo is large and the operator is exploring.

### Script mode

```
$ cat ./bootstrap.repl
collection apply users --schema schemas/user.json --indexes email
doc put users u1 --payload '{"email":"alice@example.com","name":"Alice"}'
doc put users u2 --payload '{"email":"bob@example.com","name":"Bob"}'
$ ledgerdb --repo ./data.git repl --script ./bootstrap.repl
```

Script mode is the right shape for repeatable seeds, smoke tests, and demos. The script is a sequence of CLI invocations; the REPL just makes them cheaper to run together. Use `--json` on the outer command if the script's output is being consumed by another program — every inner command will emit JSON because the flag is inherited.

## query explain

`ledgerdb query explain` is implemented in [`internal/cli/cmd_query_explain.go`](../tree/main/internal/cli/cmd_query_explain.go). It opens the SQLite sidecar read-only, runs `EXPLAIN QUERY PLAN <sql>` against it, and renders the resulting tree of plan rows. The whole file is about 150 lines and is the only place in the CLI that opens a SQLite handle outside of the indexing path.

### Invocation

```
ledgerdb query explain "<sql>" [--db <path>]
```

`--db` defaults to `<repo>/index.db` (the same default the rest of the CLI uses). The query must be a single string argument; the surrounding shell is responsible for quoting it. Output is either the rendered tree (default) or a JSON array of `{ id, parent, detail }` rows (with `--json`).

### What the plan looks like

SQLite's `EXPLAIN QUERY PLAN` returns one row per planning decision: the join order, whether a temp B-tree is needed, which index (if any) is used to satisfy a `WHERE` clause. The CLI parses each row into `explainPlanRow{ ID, Parent, Detail }` and uses the parent column to compute nesting depth (`childIndentByParent`). The rendered output is then a bulleted tree under a single `QUERY PLAN` header.

```
$ ledgerdb query explain \
    "SELECT doc_id FROM collection_invoices WHERE json_extract(payload,'$.status') = 'open' ORDER BY doc_id"
QUERY PLAN
- SCAN collection_invoices
- USE TEMP B-TREE FOR ORDER BY
```

A plan that uses an index reads differently:

```
$ ledgerdb query explain \
    "SELECT doc_id FROM collection_invoices WHERE status = 'open' ORDER BY doc_id"
QUERY PLAN
- SEARCH collection_invoices USING INDEX idx_collection_invoices_status (status=?)
```

The difference between the two is the load-bearing observation. The first query asks SQLite to read every row, decode `payload` as JSON, extract `$.status`, and compare — a full table scan whose cost grows with collection size. The second query lets SQLite use the index created by the `collection apply --indexes status` step. The index is built by `index sync` extracting the indexed field into a real column at sync time; once it exists, the planner picks it.

The point of `query explain` is therefore to confirm that the index a caller expected to be hit actually is. The two common failure modes are: an index was declared on a field name but the query references a JSON path (`json_extract(...)` does not match the indexed column), and an index was declared as composite but the query's `WHERE` ordering does not match the index's column ordering. Both are caught immediately by running the query through `query explain` before pushing it into application code.

### Reading nested plans

For multi-table or subquery plans the parent column carries the nesting. `childIndentByParent` walks the rows in order and computes a depth per `ID`. A `JOIN` shows up as a flat list of `SEARCH`/`SCAN` rows; a subquery shows up as a child block under its parent row. The current renderer does not draw connecting lines; it just indents two spaces per level. Combined with `--json` it is straightforward to feed the plan into a downstream parser if a richer visualisation is needed.

The connection is opened with `mode=ro` and `db.SetMaxOpenConns(1)` so the explain never mutates the sidecar and never holds more than one connection. That matters when running `query explain` against a database that the watch loop is concurrently writing to: the explain will block on SQLite's writer lock briefly but will not interfere with the write.

### Constraints

The query argument is `EXPLAIN QUERY PLAN`'d verbatim. SQLite will reject syntactically invalid SQL with its own error, which the CLI surfaces as a `KindInternal` error (exit code 1). The command does not run the query itself — there is no result set returned, only the plan — so a query that would be slow to execute is still cheap to explain. Use `query explain` liberally during development; the cost is bounded.

## Combining the two

The two commands compose naturally. The right pattern when adding a new collection or a new query is:

1. `ledgerdb collection apply <name> --schema <path> --indexes <fields>` to declare the collection.
2. `ledgerdb index sync --db <path>` (or a running `index watch`) to materialise the index columns.
3. `ledgerdb query explain "<sql>"` to confirm the planner picks the right index.
4. Iterate on the SQL or the index spec until the plan reads `SEARCH ... USING INDEX ...` for the rows that matter.

For interactive exploration, drive steps 3 and 4 from inside the REPL:

```
ledgerdb> query explain "SELECT * FROM collection_invoices WHERE status = 'open' ORDER BY doc_id"
QUERY PLAN
- SEARCH collection_invoices USING INDEX idx_collection_invoices_status (status=?)
ledgerdb> query explain "SELECT * FROM collection_invoices WHERE assignee = 'alice' ORDER BY doc_id"
QUERY PLAN
- SCAN collection_invoices
- USE TEMP B-TREE FOR ORDER BY
```

The second result is the cue to add `assignee` to the collection's index list. The REPL keeps the repo context across iterations; `query explain` is the fast feedback loop.

## See also

- [SDK CLI Reference](SDK-CLI-Reference) — the rest of the CLI surface.
- [SDK Go SDK](SDK-Go-SDK) — `Client.Query` and `Client.QueryPaginated` use the same SQLite sidecar that `query explain` inspects.
- [Querying and Indexing Strategy](Concepts-Indexing) — design context for the indexes that show up in the plan.
- [Operations and CLI Strategy](SDK-CLI-Reference) — why the CLI carries the explain/REPL surface rather than a separate tool.
