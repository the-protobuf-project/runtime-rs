//! The AIP-160 filter subset every driver parses. Go's `core/filter.go`.

use crate::{Error, Result};

/// One `column op value` comparison. Go's `core.Clause`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Clause {
    /// The column.
    pub column: String,
    /// One of `=`, `!=`, `>`, `>=`, `<`, `<=`.
    pub op: String,
    /// The value, unquoted.
    pub value: String,
}

const OPERATORS: [&str; 6] = [">=", "<=", "!=", "=", ">", "<"];

/// Parses a conjunction of `column op value` clauses joined by `AND`; `OR` is refused, as
/// no backend here can express it. Go's `core.ParseFilter`.
pub fn parse_filter(expr: &str) -> Result<Vec<Clause>> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Ok(Vec::new());
    }
    let chars: Vec<char> = expr.chars().collect();
    if bare_word(&chars, "OR") {
        return Err(Error::Invalid(format!(
            "filter {expr:?} uses OR, which this backend cannot express; it accepts \
             conjunctions of `column op value` joined by AND"
        )));
    }
    split_conjunction(&chars)
        .iter()
        .map(|c| parse_clause(c))
        .collect()
}

fn split_conjunction(chars: &[char]) -> Vec<String> {
    let (mut out, mut current, mut quote) = (Vec::new(), String::new(), None);
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match quote {
            Some(q) => {
                current.push(c);
                if c == q {
                    quote = None;
                }
            }
            None if c == '"' || c == '\'' => {
                quote = Some(c);
                current.push(c);
            }
            None if word_at(chars, i, "AND") => {
                out.push(std::mem::take(&mut current));
                i += 2;
            }
            None => current.push(c),
        }
        i += 1;
    }
    if !current.trim().is_empty() {
        out.push(current);
    }
    out
}

fn parse_clause(clause: &str) -> Result<Clause> {
    let clause = clause.trim();
    let chars: Vec<char> = clause.chars().collect();
    let mut quote = None;
    for i in 0..chars.len() {
        let c = chars[i];
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            continue;
        }
        if c == '"' || c == '\'' {
            quote = Some(c);
            continue;
        }
        let Some(op) = OPERATORS.iter().find(|op| starts_with(&chars[i..], op)) else {
            continue;
        };
        if i == 0 {
            continue;
        }
        let column: String = chars[..i].iter().collect::<String>().trim().to_owned();
        let raw: String = chars[i + op.len()..]
            .iter()
            .collect::<String>()
            .trim()
            .to_owned();
        if column.is_empty() || raw.is_empty() {
            return Err(Error::Invalid(format!(
                "filter clause {clause:?} is incomplete"
            )));
        }
        return Ok(Clause {
            column,
            op: (*op).to_owned(),
            value: unquote(&raw),
        });
    }
    Err(Error::Invalid(format!(
        "filter clause {clause:?} is not understood; this backend accepts conjunctions of \
         `column op value` with = != > >= < <="
    )))
}

fn starts_with(chars: &[char], s: &str) -> bool {
    let mut it = chars.iter();
    s.chars().all(|c| it.next() == Some(&c))
}

fn word_at(chars: &[char], i: usize, word: &str) -> bool {
    let n = word.chars().count();
    if i + n > chars.len() {
        return false;
    }
    let found: String = chars[i..i + n].iter().collect();
    found.eq_ignore_ascii_case(word)
        && (i == 0 || chars[i - 1] == ' ')
        && (i + n == chars.len() || chars[i + n] == ' ')
}

fn bare_word(chars: &[char], word: &str) -> bool {
    let mut quote = None;
    for (i, &c) in chars.iter().enumerate() {
        if let Some(q) = quote {
            if c == q {
                quote = None;
            }
            continue;
        }
        if c == '"' || c == '\'' {
            quote = Some(c);
            continue;
        }
        if word_at(chars, i, word) {
            return true;
        }
    }
    false
}

fn unquote(s: &str) -> String {
    let b = s.as_bytes();
    if b.len() >= 2 && (b[0] == b'"' || b[0] == b'\'') && b[b.len() - 1] == b[0] {
        return s[1..s.len() - 1].to_owned();
    }
    s.to_owned()
}
