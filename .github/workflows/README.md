# CI, releases, and package publication

[Automation overview](../README.md)

`ci.yaml` runs on main-branch pushes and pull requests. It detects crates/MSRV
from Cargo metadata, fans out per-crate build/test, verifies the combined
workspace, checks formatting/clippy (including excluded docgen), builds docs
with warnings denied, checks the generated README, builds at MSRV, and audits
dependencies. The final `ci-gate` requires every listed prerequisite to pass.

Use the corresponding local commands during development:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
just docs-check
```

`release.yaml` responds to workspace `vX.Y.Z` or crate-scoped `crate/vX.Y.Z`
tags. It tests the workspace, builds notes from Git history (scoped to the
crate directory when applicable), then creates a GitHub release. A release
tag is a publishing action; it does not automatically upload crates.io packages.

`publish.yaml` is manual, defaults to a dry run, resolves the selected package's
directory from metadata, tests it, then runs `cargo publish --dry-run` or
`cargo publish`. Actual upload uses `CARGO_REGISTRY_TOKEN` and the configured
`crates-io` environment; required reviewers depend on repository settings.
The current choice list includes network/cache/agents/grpc, not database or
telemetry. Update that list deliberately when extending publication. Publish
updated dependencies before their dependents (for example agents before grpc).

These READMEs describe checked-in workflow behavior; they do not trigger runs,
create tags, change secrets, or publish packages.
