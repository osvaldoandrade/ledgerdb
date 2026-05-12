# Java SDK

> **Status: Coming soon.** Tracked in
> [#64](https://github.com/osvaldoandrade/ledgerdb/issues/64) (part of the
> SDK epic [#59](https://github.com/osvaldoandrade/ledgerdb/issues/59)).

A Java SDK is planned to extend LedgerDB's reach into JVM ecosystems
(Spring, Quarkus, Android tooling). It will follow the same conformance
contract as the other bindings (see
[`docs/09_SDK_SPECS.md`](../09_SDK_SPECS.md)). The initial implementation
will most likely use a CLI bridge before moving to JGit + a JDBC SQLite
driver for full in-process access.

## Planned API shape

```java
import io.ledgerdb.Client;
import io.ledgerdb.Config;
import io.ledgerdb.RevertOptions;

import java.util.List;
import java.util.Map;

public class Example {
    public static void main(String[] args) throws Exception {
        try (Client client = Client.open(Config.builder()
                .repoPath(".")
                .autoSync(true)
                .build())) {

            client.put("tasks", "task_0001", Map.of(
                    "title", "Ship v1",
                    "status", "todo"));

            Map<String, Object> task = client.get("tasks", "task_0001");
            System.out.println(task);

            client.patch("tasks", "task_0001", List.of(
                    Map.of("op", "replace", "path", "/status", "value", "done")));

            client.log("tasks", "task_0001")
                  .forEach(e -> System.out.println(e.timestamp() + " " + e.op()));

            client.revert("tasks", "task_0001",
                    RevertOptions.byTxId("01HXX..."));
        }
    }
}
```

The SQLite sidecar is expected to be exposed through standard JDBC:

```java
try (var conn = client.indexConnection();
     var stmt = conn.prepareStatement(
         "SELECT doc_id FROM \"collection_tasks\" WHERE deleted = 0")) {
    try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
            System.out.println(rs.getString("doc_id"));
        }
    }
}
```

## In the meantime

You can drive LedgerDB from Java today by invoking the `ledgerdb` CLI via
`ProcessBuilder` with `--json`. The JSON contract is stable and matches what
the TypeScript SDK already uses.

## Follow progress

- Tracking issue: [#64](https://github.com/osvaldoandrade/ledgerdb/issues/64)
- Epic: [#59](https://github.com/osvaldoandrade/ledgerdb/issues/59)
- Conformance spec: [`docs/09_SDK_SPECS.md`](../09_SDK_SPECS.md)
