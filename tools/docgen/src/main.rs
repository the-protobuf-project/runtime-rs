//! Command docgen owns README.md. It reads the workspace with `cargo metadata`
//! and writes the entire README from that metadata — title, badges, crate table,
//! install lines, doc links and licence. Nothing in the file is hand-written, so
//! nothing in it can quietly drift out of step with the workspace.
//!
//! Run it from the repo root:
//!
//! ```text
//! just docs         # rewrite README.md
//! just docs-check   # validate only: fail if a crate is undocumented or the README is stale
//! ```
//!
//! The check mode is what CI runs on every push and pull request: it never
//! writes, it just exits non-zero (with a clear message) when a crate is missing
//! documentation or when README.md is not byte-for-byte what docgen would emit.
//!
//! This is the Rust counterpart of `runtime-go/tools/docgen`. Where the Go tool
//! reads each package's synopsis out of its `doc.go` comment, Cargo already has
//! a first-class place for that — `package.description`, the same text crates.io
//! shows — so summaries come from the manifests and the `//!` crate docs are
//! checked separately rather than scraped.
//!
//! Prose that cannot be derived from Cargo's own schema lives in the manifests
//! too, so it stays next to what it describes: a repo-wide `tagline` under
//! `[workspace.metadata.docgen]`, and the runtime-go package each crate ports
//! under `[package.metadata.docgen]`.
//!
//! Anything longer than a one-line summary belongs in a crate's `//!` docs,
//! where rustdoc renders it and doctests keep its examples honest — not in a
//! README, where code samples compile against nothing and rot unnoticed.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use serde_json::Value;

/// The workflow whose badge heads the README.
const CI_WORKFLOW: &str = "ci.yaml";

/// One crate in the workspace.
struct Crate {
    /// Directory relative to the workspace root, e.g. `network-graphql`.
    rel: String,
    /// Published package name, e.g. `tpp-network`.
    package: String,
    /// The runtime-go package this crate ports, from `[package.metadata.docgen]`.
    go_source: Option<String>,
    /// `package.description` from the manifest.
    description: Option<String>,
    /// The crate is published to crates.io (`publish` is not `false`).
    publishable: bool,
    /// The crate root (lib.rs / main.rs) carries `//!` documentation.
    has_doc: bool,
}

/// Everything the README is rendered from.
struct Workspace {
    /// Repo name, taken from the workspace root directory.
    title: String,
    /// One-line summary from `[workspace.metadata.docgen] tagline`.
    tagline: Option<String>,
    /// Repository URL, used for the CI badge.
    repository: Option<String>,
    /// SPDX licence expression, e.g. `Apache-2.0`.
    license: Option<String>,
    crates: Vec<Crate>,
}

fn main() -> ExitCode {
    let check = env::args()
        .skip(1)
        .any(|arg| arg == "--check" || arg == "-check");

    match run(check) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("docgen: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(check: bool) -> Result<ExitCode, String> {
    let root = find_root()?;
    let workspace = collect(&root)?;
    if workspace.crates.is_empty() {
        return Err("the workspace declares no members".into());
    }

    let undocumented: Vec<&Crate> = workspace
        .crates
        .iter()
        .filter(|c| c.description.is_none() || !c.has_doc)
        .collect();

    let readme_path = root.join("README.md");
    let current = fs::read_to_string(&readme_path).unwrap_or_default();
    let rendered = render(&workspace);

    if check {
        let mut ok = true;

        if !undocumented.is_empty() {
            ok = false;
            eprintln!("docgen: {} crate(s) are undocumented:", undocumented.len());
            for c in &undocumented {
                if c.description.is_none() {
                    eprintln!("  - {}/Cargo.toml: no `description` under [package]", c.rel);
                }
                if !c.has_doc {
                    eprintln!("  - {}: crate root has no `//!` documentation", c.rel);
                }
            }
        }

        if rendered != current {
            ok = false;
            eprintln!("docgen: README.md is out of date — run `just docs` and commit the result.");
        }

        if !ok {
            return Ok(ExitCode::FAILURE);
        }
        println!(
            "docgen: OK — {} crates documented, README.md up to date.",
            workspace.crates.len()
        );
        return Ok(ExitCode::SUCCESS);
    }

    for c in &undocumented {
        if c.description.is_none() {
            eprintln!(
                "docgen: warning: {}/Cargo.toml has no `description` under [package]",
                c.rel
            );
        }
        if !c.has_doc {
            eprintln!(
                "docgen: warning: {} crate root has no `//!` documentation",
                c.rel
            );
        }
    }

    if rendered == current {
        println!(
            "docgen: README.md already up to date ({} crates).",
            workspace.crates.len()
        );
        return Ok(ExitCode::SUCCESS);
    }

    fs::write(&readme_path, rendered)
        .map_err(|e| format!("writing {}: {e}", readme_path.display()))?;
    println!(
        "docgen: wrote README.md ({} crates).",
        workspace.crates.len()
    );
    Ok(ExitCode::SUCCESS)
}

/// Walks up from the working directory to the workspace root — the directory
/// whose `Cargo.toml` declares a `[workspace]` table.
///
/// docgen is deliberately *excluded* from that workspace (it is tooling, not a
/// published crate), so its own manifest is skipped by this search and running
/// it from `tools/docgen` still finds the real root.
fn find_root() -> Result<PathBuf, String> {
    let mut dir = env::current_dir().map_err(|e| format!("resolving working directory: {e}"))?;
    loop {
        if let Ok(text) = fs::read_to_string(dir.join("Cargo.toml"))
            && text
                .lines()
                .any(|line| line.trim_start().starts_with("[workspace]"))
        {
            return Ok(dir);
        }
        if !dir.pop() {
            return Err(
                "no Cargo.toml with a [workspace] table found in any parent directory".into(),
            );
        }
    }
}

/// Reads the workspace out of `cargo metadata`, crates sorted by directory.
fn collect(root: &Path) -> Result<Workspace, String> {
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let output = Command::new(cargo)
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .arg("--manifest-path")
        .arg(root.join("Cargo.toml"))
        .output()
        .map_err(|e| format!("running cargo metadata: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let metadata: Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| format!("parsing cargo metadata output: {e}"))?;

    let packages = metadata["packages"]
        .as_array()
        .ok_or("cargo metadata returned no `packages` array")?;

    let mut crates = Vec::with_capacity(packages.len());
    for pkg in packages {
        let manifest_path = Path::new(
            pkg["manifest_path"]
                .as_str()
                .ok_or("a package has no `manifest_path`")?,
        );
        let dir = manifest_path
            .parent()
            .ok_or_else(|| format!("{} has no parent directory", manifest_path.display()))?;
        let rel = match dir.strip_prefix(root) {
            Ok(p) if p.as_os_str().is_empty() => ".".to_string(),
            Ok(p) => p.to_string_lossy().replace('\\', "/"),
            Err(_) => dir.to_string_lossy().replace('\\', "/"),
        };

        crates.push(Crate {
            rel,
            package: pkg["name"]
                .as_str()
                .ok_or("a package has no `name`")?
                .to_string(),
            go_source: text(&pkg["metadata"]["docgen"]["go-source"]),
            description: text(&pkg["description"]),
            // `publish = false` serialises as an empty array; absent (the
            // default, meaning publishable anywhere) serialises as null.
            publishable: !matches!(&pkg["publish"], Value::Array(a) if a.is_empty()),
            has_doc: crate_root_has_doc(pkg),
        });
    }
    crates.sort_by(|a, b| a.rel.cmp(&b.rel));

    Ok(Workspace {
        title: root
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .ok_or("the workspace root has no directory name")?,
        tagline: text(&metadata["metadata"]["docgen"]["tagline"]),
        // Inherited from workspace.package, so any member carries it.
        repository: packages.iter().find_map(|p| text(&p["repository"])),
        license: packages.iter().find_map(|p| text(&p["license"])),
        crates,
    })
}

/// A trimmed, non-empty string from a JSON value, if it holds one.
fn text(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Reports whether the crate's root source file opens with `//!` docs.
///
/// Only the crate root is inspected: `#![warn(missing_docs)]` in each crate
/// already forces every *public item* to be documented, and the `docs` CI job
/// promotes that warning to an error. The one thing that lint cannot catch is a
/// crate with no crate-level doc at all, which is exactly what this covers.
fn crate_root_has_doc(pkg: &Value) -> bool {
    let Some(targets) = pkg["targets"].as_array() else {
        return false;
    };

    let has_kind = |target: &Value, want: &str| {
        target["kind"]
            .as_array()
            .is_some_and(|kinds| kinds.iter().any(|k| k == want))
    };

    // Prefer the library target; a crate that only ships a binary is checked on
    // that instead, so tooling crates are not silently exempt.
    let root = targets
        .iter()
        .find(|t| has_kind(t, "lib") || has_kind(t, "rlib"))
        .or_else(|| targets.iter().find(|t| has_kind(t, "bin")));

    let Some(src) = root.and_then(|t| t["src_path"].as_str()) else {
        return false;
    };

    fs::read_to_string(src).is_ok_and(|text| {
        text.lines()
            .any(|line| line.trim_start().starts_with("//!"))
    })
}

/// Renders the complete README.
fn render(ws: &Workspace) -> String {
    let mut b = String::new();

    b.push_str("<!-- Generated by tools/docgen from Cargo metadata. Do not edit by hand: -->\n");
    b.push_str("<!-- run `just docs` instead. Per-crate prose belongs in that crate's -->\n");
    b.push_str("<!-- `//!` docs, where rustdoc renders it and doctests keep it honest. -->\n\n");

    b.push_str(&format!("# {}\n\n", ws.title));

    if let Some(repo) = &ws.repository {
        b.push_str(&format!(
            "[![CI]({repo}/actions/workflows/{CI_WORKFLOW}/badge.svg)]({repo}/actions/workflows/{CI_WORKFLOW})\n"
        ));
    }
    if let Some(license) = &ws.license {
        // shields.io reads a single dash as a field separator, so escape it.
        let label = license.replace('-', "--");
        b.push_str(&format!(
            "[![License: {license}](https://img.shields.io/badge/license-{label}-blue.svg)](LICENSE)\n"
        ));
    }
    if ws.repository.is_some() || ws.license.is_some() {
        b.push('\n');
    }

    if let Some(tagline) = &ws.tagline {
        b.push_str(tagline);
        b.push_str("\n\n");
    }

    b.push_str("## Crates\n\n");
    b.push_str(&format!(
        "{} crates. Each declares a `description` in its `Cargo.toml` and carries crate-level\n",
        ws.crates.len()
    ));
    b.push_str("`//!` docs; this table is generated from those and verified on every push.\n\n");
    b.push_str("| Crate | Published as | Go source | Summary |\n");
    b.push_str("| --- | --- | --- | --- |\n");
    for c in &ws.crates {
        b.push_str(&format!(
            "| [`{}`]({}) | `{}` | {} | {} |\n",
            c.rel,
            c.rel,
            c.package,
            c.go_source
                .as_ref()
                .map_or_else(|| "—".to_string(), |s| format!("`{s}`")),
            c.description.as_deref().unwrap_or("_(no description)_"),
        ));
    }
    b.push('\n');

    let published: Vec<&Crate> = ws.crates.iter().filter(|c| c.publishable).collect();
    if !published.is_empty() {
        b.push_str("## Installation\n\n```bash\n");
        let width = published
            .iter()
            .map(|c| c.package.len())
            .max()
            .unwrap_or_default();
        for c in &published {
            match &c.description {
                Some(d) => b.push_str(&format!(
                    "cargo add {:<width$}  # {}\n",
                    c.package,
                    d,
                    width = width
                )),
                None => b.push_str(&format!("cargo add {}\n", c.package)),
            }
        }
        b.push_str("```\n\n");

        b.push_str("## Documentation\n\n");
        b.push_str("Every crate's API docs, including usage examples, are on docs.rs:\n\n");
        for c in &published {
            b.push_str(&format!("- [`{0}`](https://docs.rs/{0})\n", c.package));
        }
        b.push('\n');
    }

    b.push_str("## Development\n\n```bash\n");
    b.push_str("just              # list the available recipes\n");
    b.push_str("just docs         # regenerate this README from Cargo metadata\n");
    b.push_str("just docs-check   # verify it is current (also runs in CI)\n");
    b.push_str("```\n");

    if let Some(license) = &ws.license {
        b.push_str(&format!(
            "\n## License\n\n{license} — see [LICENSE](LICENSE).\n"
        ));
    }

    b
}
