//! Assembling the whole banner: the art, the build line, the table, and the warnings under it.

use owo_colors::Style;

use crate::options::Options;

use super::style::{BUILD_REVISION, environment_label, paint, section_style};
use super::summary::Summary;
use super::table::Row;
use super::{figure, summary, table};

/// Renders the complete banner as a string.
///
/// It returns rather than prints so a host can log it, test it, or send it somewhere other than
/// stdout — [`print`](fn@print) is the convenience over it.
pub fn render(opts: &Options, state: &Summary) -> String {
    let mut out = String::new();

    for line in figure::render(&opts.service_name) {
        out.push_str(&paint(&line, Style::new().magenta().bold()));
        out.push('\n');
    }

    out.push('\n');
    out.push_str(&build_line(opts));
    out.push_str("\n\n");

    let rows = summary::rows(opts, state);
    out.push_str(&table::render(&coloured(rows)));
    out.push('\n');

    // Warnings sit below the table so they are the last thing read, rather than being scrolled
    // past on the way to the summary they qualify.
    for warning in warnings(opts, state) {
        out.push_str(&paint(&format!("WARN  {warning}"), Style::new().yellow()));
        out.push('\n');
    }

    out
}

/// Prints the banner to stdout.
pub fn print(opts: &Options, state: &Summary) {
    print!("{}", render(opts, state));
}

/// The version, environment and build line under the art.
fn build_line(opts: &Options) -> String {
    let dim = Style::new().dimmed();
    let separator = paint(" * ", dim);
    format!(
        "{}{separator}Environment: {}{separator}{}",
        paint(
            &format!("Version: {}", opts.version),
            Style::new().white().bold()
        ),
        environment_label(opts.environment),
        paint(&format!("Build: #{BUILD_REVISION}"), dim),
    )
}

/// Colours each row's section label.
///
/// The table measures width in display columns, so this happens after the rows are built but
/// the padding is computed from the uncoloured text — escape sequences occupy no columns, and
/// counting them would make every section cell short.
fn coloured(rows: Vec<Row>) -> Vec<Row> {
    rows.into_iter()
        .map(|row| Row {
            section: paint(&row.section, section_style(&row.section)),
            ..row
        })
        .collect()
}

/// What a reader should know that the table does not say.
fn warnings(opts: &Options, state: &Summary) -> Vec<String> {
    let mut out = Vec::new();
    if !state.tls {
        out.push("gRPC: running without TLS (plaintext)".to_string());
    }
    if opts.enable_health {
        out.push("Health check service enabled".to_string());
    }
    if state.reflection {
        out.push("Reflection service enabled (dev/debug only)".to_string());
    }
    out
}
