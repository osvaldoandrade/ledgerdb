<!--
Thanks for contributing to LedgerDB! Please fill in the sections below.
See CONTRIBUTING.md for the full guidelines (small focused PRs, conventional
commits, DCO sign-off).
-->

## Summary

<!--
Why this change? 1–3 bullets. Focus on motivation, not a diff replay.
-->

-
-

## Test plan

<!--
How you verified the change. Tick the boxes that apply and add commands.
-->

- [ ] `go test ./...` passes locally
- [ ] `go test -race ./...` passes locally (required for concurrency changes)
- [ ] `go vet ./...` and `golangci-lint run` are clean
- [ ] Integration scripts under `tests/` exercised where relevant
- [ ] Manual verification (commands below):

```
# paste the commands you ran
```

## Linked issue

<!--
Use GitHub's auto-close syntax. Use Refs #N for work-in-progress.
-->

Closes #

## Checklist

- [ ] Tests added or updated to cover the change
- [ ] Docs updated if user-visible (README, project wiki, command help text)
- [ ] No breaking changes — or, if there are, they are called out below
      and follow the [Deprecation Policy](https://github.com/osvaldoandrade/ledgerdb/wiki/Stability-Deprecation)
- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
- [ ] DCO sign-off present (`git commit -s`)

### Breaking changes

<!--
Delete this section if there are none. Otherwise, describe:
- What changed
- What downstream users must do to migrate
- Which deprecation cycle this lands in
  (see https://github.com/osvaldoandrade/ledgerdb/wiki/Stability-Deprecation)
-->
