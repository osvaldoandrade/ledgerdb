# LedgerDB Governance

This document describes how decisions are made in the LedgerDB project, who
makes them, and how that group can evolve over time. It is deliberately
lightweight: LedgerDB is a young project and over-engineering governance
before it is needed slows things down. We will revise this document as the
community grows.

---

## 1. Current maintainer

LedgerDB is currently maintained by:

- **Osvaldo Andrade** (project lead) — final decision maker on roadmap,
  releases, and any change touching the TxV3 on-disk format.

The maintainer is responsible for:

- Triaging issues and reviewing pull requests.
- Cutting releases (see `.github/workflows/release.yml`).
- Curating the roadmap (`ROADMAP.md`).
- Enforcing the Code of Conduct.
- Coordinating security disclosures (see `SECURITY.md`).

As the project grows we expect this to expand to a small team. Sections 4
and 5 describe how that happens.

---

## 2. Decision model

LedgerDB operates on **lazy consensus** in the open.

1. Proposals are made via a GitHub issue or pull request.
2. The proposal is visible to anyone for **at least 7 calendar days** before
   it is considered accepted, regardless of how few or how many comments it
   receives.
3. If no maintainer or contributor raises a substantive objection within
   that window, the proposal is considered accepted by lazy consensus.
4. If an objection is raised, discussion continues until either the
   objection is withdrawn, the proposal is amended to address it, or — in
   the rare case of irreconcilable disagreement — the maintainer makes a
   final call and records the reasoning on the issue.

Lazy consensus applies to all routine work: features, bug fixes, refactors,
documentation, CI changes, and dependency updates. The 7-day window may be
shortened by the maintainer for trivial or time-sensitive changes (typo
fixes, security patches, CI breakage), and lengthened for sweeping or
contentious proposals.

### 2.1 What does **not** use lazy consensus

The following changes require explicit maintainer approval and cannot be
merged by lazy consensus alone:

- Changes to the TxV3 wire format (`internal/infra/txv3/`).
- Changes to the public Go SDK surface (`pkg/ledgerdbsdk/`).
- Changes to the CLI command surface that remove or rename existing
  commands or flags.
- Changes to this document, the Code of Conduct, or the Security policy.
- Anything affecting the release pipeline or signed artifacts.

---

## 3. RFC / ADR process

Substantial design decisions — new subsystems, new external protocols, or
anything that changes the data model — should be proposed as an **RFC** (also
known as an ADR, *architecture decision record*).

Workflow:

1. **Open a GitHub Issue** tagged `rfc` with the proposal. The issue body
   should cover:
   - Context and problem statement.
   - Proposed design, with diagrams where useful.
   - Alternatives considered and why they were rejected.
   - Compatibility, migration, and rollout plan.
   - Open questions.
2. Link any related issues. Comment-driven discussion lives on the issue
   thread.
3. The standard 7-day lazy-consensus window applies. The maintainer will
   either accept the proposal (closing the issue with an `accepted` label)
   or request changes.
4. **Once accepted, the maintainer commits the accepted design as a new
   wiki page** under the relevant section of the project wiki
   ([github.com/osvaldoandrade/ledgerdb/wiki](https://github.com/osvaldoandrade/ledgerdb/wiki)).
   The wiki page is the canonical record going forward; the issue thread
   stays linked from the page for historical context.
5. Implementation PRs link back to the accepted issue and the resulting
   wiki page.

For very small design choices a short note in an issue is usually sufficient
— RFCs are for changes that future contributors will want to *understand
later*. The wiki is the durable home for those decisions because it is
editable in place as designs evolve and because it is the same surface the
reference documentation already lives in; a separate, frozen ADR tree would
drift out of sync with the running wiki within a release or two.

---

## 4. Escalation

If you believe a decision was made incorrectly, a discussion has stalled, or
the maintainer is not responding within a reasonable time:

1. Open a GitHub issue with the label **`governance`**, briefly describing
   the situation and what you would like to happen.
2. The maintainer commits to responding to `governance`-tagged issues
   within **14 calendar days**.
3. If the matter involves the maintainer personally (conflict of interest,
   Code of Conduct concern), follow the reporting path in
   `CODE_OF_CONDUCT.md` instead.

We intentionally keep escalation simple while the project has a single
maintainer; this section will be expanded when additional maintainers are
appointed.

---

## 5. Becoming a maintainer

Maintainership is earned through **sustained, high-quality contribution**.
There is no formal application process. The signals current maintainers
look for include:

- Consistent PR contributions over several months across more than one
  subsystem.
- Helpful, accurate code review on others' PRs.
- Triaging and reproducing reported bugs.
- Engaging constructively in design discussions.
- Demonstrating good judgement on when to seek consensus and when to act.

Process:

1. An existing maintainer nominates a candidate by opening an issue tagged
   `governance` and `maintainer-nomination`.
2. The nomination summarises the candidate's contributions and links to
   representative work.
3. The standard 7-day lazy-consensus window applies. Other maintainers and
   the wider community may comment.
4. If accepted, the candidate is added to:
   - This file.
   - `CODEOWNERS` (when introduced).
   - The GitHub team with merge rights.

Maintainers may step down at any time by opening a PR removing themselves
from this file. Inactive maintainers (no review or commit activity for
twelve months) may be moved to an "emeritus" list by lazy consensus among
the remaining active maintainers.

---

## 6. Changing this document

Changes to `GOVERNANCE.md` follow the rules in section 2.1: they require
explicit maintainer approval, not lazy consensus, and should be proposed as
a pull request linked to a `governance`-tagged issue.
