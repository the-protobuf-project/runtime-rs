# Root README generator

[Tools](../README.md) · [Implementation](src/README.md)

This unpublished standalone Cargo package owns the repository-root README.
Run it from within the repository; it walks upward to the Cargo workspace.

```bash
just docs
just docs-check
cargo run --manifest-path tools/docgen/Cargo.toml -- --check
```

The root manifest's `[workspace.metadata.docgen]` supplies the tagline. Each
crate's package name/description/repository/license and
`[package.metadata.docgen].go-source` supply generated tables, install lines,
and API links. The tool checks library/binary roots for `//!` docs.

`cargo metadata --no-deps` -> collect/sort members -> inspect crate roots ->
render complete README -> compare or write. Normal mode changes only the root
README, warning about undocumented crates. `--check` writes nothing and fails
on missing crate description/docs or stale generated content. It does not
prove that all public item docs or README snippets compile; rustdoc and targeted
snippet checks cover those separately.

Do not hand-edit generated root content. Change manifest inputs or the renderer,
run `just docs`, then `just docs-check`. Folder guides remain hand-maintained.
Docgen is excluded from the workspace, so format/lint it explicitly:

```bash
cargo fmt --manifest-path tools/docgen/Cargo.toml --check
cargo clippy --manifest-path tools/docgen/Cargo.toml --all-targets -- -D warnings
```
