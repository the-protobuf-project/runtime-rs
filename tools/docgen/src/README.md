# README collection and rendering

[Tool usage](../README.md)

`main.rs` implements the whole small command. `main` parses check mode and
converts errors to exit codes. `find_root` walks parents for a workspace
manifest, skipping docgen's standalone manifest. `collect` invokes Cargo
metadata, extracts crate/manifests, checks root `//!` docs, and sorts entries
by directory. `render` generates deterministic Markdown from that model.
`run` compares current/rendered content and either reports freshness or writes.

```bash
cargo run --manifest-path tools/docgen/Cargo.toml --
cargo run --manifest-path tools/docgen/Cargo.toml -- --check
```

The `CARGO` environment variable can select the Cargo executable used for
metadata. The tool reports missing workspace/metadata/read/write failures and
returns nonzero rather than silently producing an empty inventory.

When changing rendered prose or links, update `render`, regenerate the root
README, and run check mode again. Keep source examples in the applicable
module guides/rustdoc; this renderer owns navigation and metadata-derived
workspace content. Ordinary invocation does not generate per-folder files.
