# Security Policy

LedgerDB's integrity guarantees are a core feature: a tampered ledger is no
longer a ledger. We take security reports seriously and ask reporters to do
the same.

---

## 1. Supported versions

Only the **latest published 0.2.x** release receives security fixes. Older
0.x lines are not maintained.

| Version       | Supported           |
|---------------|---------------------|
| 0.2.x (latest)| Yes — active patches|
| 0.2.x (older) | No                  |
| 0.1.x         | No                  |
| < 0.1         | No                  |

After v1.0 we will switch to a more conservative window (current minor +
previous minor); the policy will be revised here when that happens.

---

## 2. Reporting a vulnerability

**Please do not file public GitHub issues for security bugs.**

Send reports to **security@ledgerdb.org** (placeholder — replace with a
real address before publishing). Encrypt with our GPG key if you can; see
section 4.

Include, where applicable:

- A description of the vulnerability and its impact.
- The affected version(s) and platform(s).
- Steps to reproduce, ideally as a minimal repository or test case.
- Whether the issue is already publicly known.
- Whether you intend to publish your own write-up, and on what timeline.

You should receive an acknowledgement within **3 business days**. If you do
not, please re-send and CC the project maintainer listed in `GOVERNANCE.md`.

---

## 3. Coordinated disclosure

LedgerDB follows a **90-day coordinated disclosure window**.

1. **Day 0**: report received and acknowledged.
2. **Day 0–7**: triage and severity assessment (see section 5).
3. **Day 7–60**: fix developed, tested, and reviewed; patched release
   prepared.
4. **Day 60–90**: patched release published; reporter and downstream
   integrators notified; CVE requested where applicable.
5. **Day 90 (or earlier if patched)**: public advisory published in this
   repository's GitHub Security Advisories.

If a fix is ready earlier, we will publish earlier and credit the reporter
(unless they request anonymity). If a fix requires a longer window — for
example, an on-disk format change with migration tooling — we will agree on
an extended timeline with the reporter in writing.

If a vulnerability is being actively exploited in the wild, we may publish
without waiting for the 90-day window. We will notify the reporter before
doing so.

---

## 4. GPG key

A project GPG key for encrypted reports is **TBD — see the latest release
notes** for the current fingerprint and a download link. Until that key is
published, plain email to `security@ledgerdb.org` is acceptable; please do
not include exploit details that you would not be comfortable sending in
the clear, and tell us you have a write-up so we can establish a more
secure channel.

---

## 5. Severity rubric

We classify reports against the following rubric. These categories drive
both the patching timeline and the visibility of the resulting advisory.

### Critical
- A break of **TxV3 integrity** — for example, an attacker can produce a
  TxV3 blob whose computed hash collides with, or forges the chain of, a
  legitimate one.
- Remote code execution in the CLI or SDK from attacker-controlled input
  (malformed schema, crafted document, malicious remote).
- Authentication or signing bypass that lets an unauthorised actor write
  to the ledger and have it accepted as legitimate.

Target patch: within **7 days** of confirmation.

### High
- **Unauthorised data exposure** — for example, the CLI or SDK leaking
  payloads, schemas, or signing keys through logs, error messages, or
  side channels.
- A flaw that lets an attacker silently roll the ledger back to an earlier
  state on a victim's clone.
- Privilege escalation in any opt-in network surface (the local web
  console, future RPC endpoints).

Target patch: within **30 days**.

### Medium
- **Availability impact** — denial-of-service against the CLI, the watcher,
  or the sidecar (crash, livelock, unbounded memory growth) triggered by
  attacker-influenced inputs.
- Integrity bugs that are detectable by `ledgerdb integrity verify` but
  could mislead a casual operator.

Target patch: within **60 days**.

### Low
- **Deniability of writes** — flaws that make it harder for the legitimate
  author to prove a write was theirs, without enabling forgery.
- Information disclosure of non-sensitive metadata (timing, byte counts).
- Hardening opportunities with no known exploit path.

Target patch: in the next regular release.

---

## 6. Safe harbour

We will not pursue legal action against researchers who:

- Make a good-faith effort to comply with this policy.
- Avoid privacy violations, data destruction, and service degradation
  beyond what is strictly necessary to demonstrate the issue.
- Give us the disclosure window agreed in section 3.

If you are unsure whether a given test would be in scope, ask first at
`security@ledgerdb.org`.
