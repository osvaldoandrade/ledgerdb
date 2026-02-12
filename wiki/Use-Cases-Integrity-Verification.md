# Use Case: Integrity Verification

Verify complete history for tamper evidence.

## Actors

- Auditor or operator
- Integrity verifier
- Git object store

## Preconditions

- Repository contains full object history.

## Main flow

1. Operator starts integrity verification.
2. Verifier loads genesis and iterates transaction chain.
3. For each transaction, hash is recomputed deterministically.
4. Parent-child linkage is validated across chain.
5. Verification report returns success or first mismatch.

### Sequence diagram

```mermaid
sequenceDiagram
    participant O as Operator
    participant V as Verifier
    participant G as Git Object Store

    O->>V: integrity verify --deep
    V->>G: Load genesis/head and tx blobs
    loop For each tx in chain
        V->>V: Recompute hash
        V->>V: Validate parent linkage
    end
    V-->>O: Integrity report
```

## Expected outcomes

- Any tampered object breaks verification deterministically.
- Auditability is machine-checkable end-to-end.
