//! Colour and the small string helpers the summary is assembled from.
//!
//! Colour is applied only when stdout is a terminal. The Go runtime gets that from
//! `fatih/color`, which disables itself when piped; without the same guard, a captured log
//! would be full of escape sequences.

use owo_colors::{OwoColorize, Style};

use crate::options::ServerEnvironment;

/// The revision this binary was built from, captured by `build.rs`.
pub const BUILD_REVISION: &str = env!("TPP_GRPC_BUILD_REV");

/// Whether to emit escape sequences at all.
fn colour_enabled() -> bool {
    // A terminal is the only place colour helps, and NO_COLOR is the convention for turning it
    // off even there.
    std::io::IsTerminal::is_terminal(&std::io::stdout()) && std::env::var_os("NO_COLOR").is_none()
}

/// Applies `style` to `text`, or returns it unchanged when colour is off.
pub fn paint(text: &str, style: Style) -> String {
    if colour_enabled() {
        text.style(style).to_string()
    } else {
        text.to_string()
    }
}

/// The colour for a section label — each protocol layer gets its own tint.
pub fn section_style(section: &str) -> Style {
    let style = Style::new().bold();
    match section {
        s if s.starts_with("gRPC") => style.cyan(),
        s if s.starts_with("MCP") => style.yellow(),
        s if s.starts_with("A2A") => style.magenta(),
        _ => style.green(),
    }
}

/// The environment label, uppercased and coloured by how much caution it deserves.
pub fn environment_label(environment: ServerEnvironment) -> String {
    let text = environment.as_str().to_uppercase();
    let style = match environment {
        ServerEnvironment::Production => Style::new().red().bold(),
        ServerEnvironment::Staging => Style::new().yellow().bold(),
        ServerEnvironment::Debug | ServerEnvironment::Development => Style::new().green().bold(),
    };
    paint(&text, style)
}

/// Whether the transport is encrypted, as the summary puts it.
pub fn tls_label(has_tls: bool) -> &'static str {
    if has_tls { "TLS ✓" } else { "plaintext" }
}

/// `n` of `word`, pluralised by adding an `s`.
///
/// Enough for the two nouns the banner counts; a word with an irregular plural would need
/// saying properly rather than passing through here.
pub fn pluralise(n: usize, word: &str) -> String {
    if n == 1 {
        format!("1 {word}")
    } else {
        format!("{n} {word}s")
    }
}

/// The gRPC service names worth listing, sorted, with the built-ins removed.
///
/// Health and reflection have rows of their own, so listing them here would say the same thing
/// twice.
pub fn user_service_names<I: IntoIterator<Item = String>>(names: I) -> Vec<String> {
    let mut names: Vec<String> = names
        .into_iter()
        .filter(|name| {
            !matches!(
                name.as_str(),
                "grpc.health.v1.Health"
                    | "grpc.reflection.v1.ServerReflection"
                    | "grpc.reflection.v1alpha.ServerReflection"
            )
        })
        .collect();
    names.sort();
    names
}
