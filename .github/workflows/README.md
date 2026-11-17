# CI, releases, and package publication

[Automation overview](../README.md)

`ci.yaml` runs on main-branch pushes and pull requests. It detects crates/MSRV
from Cargo metadata, fans out per-crate build/test, verifies the combined
workspace, tests tpp-database's feature-gated SQL backend (SQLite in-process,
then Postgres and MySQL as service containers), checks formatting/clippy (including excluded docgen), builds docs
with warnings denied, checks the generated README, builds at MSRV, and audits
dependencies. `database-cached` lints, documents and tests tpp-database's
off-by-default `cached` feature, its live suite against a Redis service on
`127.0.0.1:16379`. The final `ci-gate` requires every listed prerequisite to pass.
dependencies. Lint and docs also cover `tpp-database` with all features, whose
optional `mongodb` backend the workspace run never compiles. The `live` job
starts `database/docker/compose.mongodb.live.yaml` (a single-node MongoDB
replica set) and runs `cargo test -p tpp-database --all-features`, so the
`mongodb_live` target exercises a real server and fails rather than skips
without one. The final `ci-gate` requires every listed prerequisite to pass.

Use the corresponding local commands during development:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
just docs-check
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
cargo test -p tpp-database --features cached,live-tests --test cached --test cached_capabilities --test cached_live
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml down
# The live job, with its server:
docker compose -p runtime-database-mongodb-live -f database/docker/compose.mongodb.live.yaml up -d --wait mongodb
cargo test -p tpp-database --all-features
docker compose -p runtime-database-mongodb-live -f database/docker/compose.mongodb.live.yaml down
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
