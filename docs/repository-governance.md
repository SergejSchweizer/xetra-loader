# Repository governance

## Contents

- [Current delivery rules](#current-delivery-rules)
- [Archived XDL-PR006/XDL-PR035 record](#archived-xdl-pr006xdl-pr035-record)

## Current delivery rules

- Run Ruff, Mypy, unit tests, and the applicable PostgreSQL integration tests
  locally as documented in `README.md`. GitHub Actions is not a required test
  runner or a delivery dependency; do not invoke or depend on it.
- Keep active, explicitly assigned work items isolated on their own branch.
  `BACKLOG.md` currently contains only obsolete PR history and no active work
  items.
- Use Conventional Commit subjects with the assigned work-item scope when one
  exists. Do not commit secrets, generated market data, or local configuration.
- The stated delivery target is `origin/main` after local gates pass. Inspect
  current remote protection before pushing; if it rejects the authorized
  operation, stop and request maintainer direction rather than force-pushing,
  weakening protections, or invoking a workflow.

The following records document retired XDL-PR process requirements. They are
historical evidence, not current delivery instructions.

## Archived XDL-PR006/XDL-PR035 record

### XDL-PR035 repair evidence

Observed before the XDL-PR035 repair (2026-08-24): GitHub returned `404 Branch not
protected` for `main`, and the repository reported `allow_auto_merge=false`.

Verified after the repair: GitHub reports `main.protected=true`, requires the exact
`merge-gate` check with strict current-head enforcement, requires pull requests,
enforces protections for administrators, blocks force pushes and branch deletion,
requires conversation resolution, and reports `allow_auto_merge=true`.

GitHub enforces direct-push rejection through this protection. The enforcement
configuration above is the safe verification evidence; attempting a known-invalid
push solely to prove rejection is not part of normal repository operation.

### Archived required `main` protections

The GitHub repository configuration is part of the XDL-PR006 acceptance contract and must enforce all of the following:

- pull requests are required before changes can reach `main`;
- the required status check is exactly `merge-gate` from `.github/workflows/merge-quality.yml`;
- required checks must pass on the current PR head before merge;
- direct feature pushes to `main` are blocked;
- force pushes are blocked;
- branch deletion is blocked;
- repository auto-merge is enabled.

The workflows intentionally keep `lint`, `type`, `unit`, `integration`, and `policy` independent. The final `merge-gate` succeeds only if every required job reports `success`.

### Archived auto-merge procedure

1. Create a work-order branch from its exact merged dependency SHA.
2. Push only commits whose Conventional Commit scope is the exact `xdl-prNNN-*` work-order.
3. Open a PR whose title uses the same exact work-order scope.
4. Wait for the push and merge quality workflows to complete on the current head SHA.
5. Once the PR is review-ready, enable GitHub auto-merge.
6. Do not manually merge around a failing, cancelled, skipped, or pending `merge-gate`.

### Archived verification checklist

XDL-PR006 is complete only after GitHub reports the repository settings above as active and a representative PR demonstrates both states:

- merge is blocked while `merge-gate` is pending or failing;
- auto-merge completes only after all required checks, including `merge-gate`, pass.

Repository settings are operational state, not secrets. Provider tokens, database passwords, and full DSNs must never be committed while exercising this procedure.
