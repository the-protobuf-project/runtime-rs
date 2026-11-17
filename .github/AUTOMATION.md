# Repository automation

[Workspace guide](../README.md) · [Workflows](workflows/README.md)

`dependabot.yaml` requests weekly Cargo-workspace and GitHub Actions updates,
with dependency labels and scoped commit prefixes. Workflow files define
validation, tag-triggered GitHub releases, and manually selected crates.io
publishing. Merely editing documentation does not execute a release.

Run the local documentation checks before opening a change:

```bash
just docs-check
cargo doc --workspace --no-deps
```

CI discovers workspace crates from Cargo metadata; `tools/docgen` remains an
excluded developer tool with separate checks. Add a new module's folder guide
and rustdoc alongside its manifest metadata, then regenerate the root README.
Workflow trigger/scope details and publish limitations are in the child guide.
