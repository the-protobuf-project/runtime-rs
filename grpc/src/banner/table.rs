//! The startup summary table, rendered to match the Go runtime's byte for byte.
//!
//! `tablewriter`'s hierarchical merge has no equivalent in the Rust table crates: a repeated
//! value in the first column is blanked, *and* the separator beneath it becomes a continuation
//! rather than a full rule. That second half is what no crate here does, so the table is drawn
//! directly.

use unicode_width::UnicodeWidthStr;

/// The columns a summary row carries.
pub const HEADERS: [&str; 4] = ["Section", "Component", "Address", "Detail"];

/// One line of the summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The protocol layer, repeated rows of which merge together.
    pub section: String,
    /// What is answering.
    pub component: String,
    /// Where it listens.
    pub address: String,
    /// Anything else worth printing.
    pub detail: String,
}

impl Row {
    /// A row from its four columns.
    pub fn new(
        section: impl Into<String>,
        component: impl Into<String>,
        address: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            section: section.into(),
            component: component.into(),
            address: address.into(),
            detail: detail.into(),
        }
    }

    fn cells(&self) -> [&str; 4] {
        [&self.section, &self.component, &self.address, &self.detail]
    }
}

/// Renders the table, including its border.
///
/// Returns an empty string for no rows, so a server with nothing to report prints no frame
/// around the nothing.
pub fn render(rows: &[Row]) -> String {
    if rows.is_empty() {
        return String::new();
    }

    // Every column is as wide as its widest cell, header included, plus one space of padding
    // either side. Width is measured in display columns rather than bytes, or a row carrying
    // "✓" would be padded one short.
    let widths: Vec<usize> = (0..HEADERS.len())
        .map(|i| {
            let header = display_width(&HEADERS[i].to_uppercase());
            rows.iter()
                .map(|row| display_width(row.cells()[i]))
                .chain(std::iter::once(header))
                .max()
                .unwrap_or(0)
        })
        .collect();

    let mut out = String::new();
    out.push_str(&rule('┌', '┬', '┐', &widths));
    out.push_str(&line(
        &HEADERS.map(|h| h.to_uppercase()),
        &widths,
        Align::Center,
    ));
    out.push_str(&rule('├', '┼', '┤', &widths));

    for (i, row) in rows.iter().enumerate() {
        // A row whose section matches the one above shows an empty section cell: the value is
        // already stated, and repeating it turns a grouping into noise.
        let merged = i > 0 && rows[i - 1].section == row.section;
        let cells = row.cells().map(str::to_string);
        let shown = if merged {
            [
                String::new(),
                cells[1].clone(),
                cells[2].clone(),
                cells[3].clone(),
            ]
        } else {
            cells
        };
        out.push_str(&line(&shown, &widths, Align::Left));

        match rows.get(i + 1) {
            None => out.push_str(&rule('└', '┴', '┘', &widths)),
            // The separator continues the section's box rather than closing it, which is what
            // makes several rows read as one group.
            Some(next) if next.section == row.section => {
                out.push_str(&continuation(&widths));
            }
            Some(_) => out.push_str(&rule('├', '┼', '┤', &widths)),
        }
    }

    out
}

/// Which way a cell's padding falls.
#[derive(Clone, Copy)]
enum Align {
    Left,
    Center,
}

/// A horizontal rule, with the given corner and junction characters.
fn rule(left: char, mid: char, right: char, widths: &[usize]) -> String {
    let spans: Vec<String> = widths.iter().map(|w| "─".repeat(w + 2)).collect();
    format!("{left}{}{right}\n", spans.join(&mid.to_string()))
}

/// The separator under a row whose section continues, leaving the first column's box unbroken.
fn continuation(widths: &[usize]) -> String {
    let mut out = format!("│{}├", " ".repeat(widths[0] + 2));
    let spans: Vec<String> = widths[1..].iter().map(|w| "─".repeat(w + 2)).collect();
    out.push_str(&spans.join("┼"));
    out.push_str("┤\n");
    out
}

/// One row of cells, padded to the column widths.
fn line(cells: &[String], widths: &[usize], align: Align) -> String {
    let mut out = String::from("│");
    for (cell, width) in cells.iter().zip(widths) {
        let pad = width.saturating_sub(display_width(cell));
        match align {
            Align::Left => out.push_str(&format!(" {cell}{} ", " ".repeat(pad))),
            Align::Center => {
                let left = pad / 2;
                out.push_str(&format!(
                    " {}{cell}{} ",
                    " ".repeat(left),
                    " ".repeat(pad - left)
                ));
            }
        }
        out.push('│');
    }
    out.push('\n');
    out
}

/// The columns `text` occupies on a terminal.
///
/// CSI escape sequences — the colour codes a section label carries — are skipped, because
/// they occupy no columns. Counting them would pad every coloured cell short.
fn display_width(text: &str) -> usize {
    let mut visible = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            // Parameter and intermediate bytes, up to and including the final byte.
            for c in chars.by_ref() {
                if ('\u{40}'..='\u{7e}').contains(&c) {
                    break;
                }
            }
            continue;
        }
        visible.push(c);
    }
    visible.width()
}

#[cfg(test)]
mod tests {
    use super::{Row, display_width, render};
    use unicode_width::UnicodeWidthStr;

    #[test]
    fn escape_sequences_occupy_no_columns() {
        assert_eq!(display_width("\u{1b}[1;36mgRPC\u{1b}[0m"), 4);
        assert_eq!(display_width("TLS ✓"), 5);
    }

    #[test]
    fn a_pre_coloured_section_keeps_every_line_the_same_width() {
        let rows = vec![
            Row::new(
                "\u{1b}[1;36mgRPC\u{1b}[0m",
                "Health Check",
                "0.0.0.0:50051",
                "grpc.health.v1",
            ),
            Row::new(
                "\u{1b}[1;36mgRPC\u{1b}[0m",
                "Reflection",
                "0.0.0.0:50051",
                "grpcurl",
            ),
            Row::new(
                "\u{1b}[1;35mHTTP/1.1\u{1b}[0m",
                "REST",
                "0.0.0.0:8080",
                "http",
            ),
        ];
        let table = render(&rows);
        let widths: Vec<usize> = table.lines().map(display_width).collect();
        assert!(
            widths.windows(2).all(|pair| pair[0] == pair[1]),
            "lines differ in width {widths:?}:\n{table}"
        );
        // The plain width check would have failed: the escapes are really there.
        assert!(table.lines().any(|line| line.width() != widths[0]));
    }
}
