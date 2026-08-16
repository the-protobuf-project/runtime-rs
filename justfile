# runtime-rs developer tasks.

docgen := "cargo run --quiet --manifest-path tools/docgen/Cargo.toml --"

# Show the available recipes.
default:
    @just --list

# Regenerate the README crate table from cargo metadata.
docs:
    {{ docgen }}

# Fail if a crate is undocumented or the README table is stale (CI).
docs-check:
    {{ docgen }} --check
