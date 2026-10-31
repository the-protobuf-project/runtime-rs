//! The AIP-160 filter subset every backend accepts, ported from runtime-go.
//!
//! Conjunctions of `column op value` joined by AND, with = != > >= < <=.
//! Small on purpose: a backend that took the whole grammar and honored part of
//! it would return the wrong records with nothing to say so. OR is refused by
//! name. Values may be quoted with " or '; they are typed by the column's
//! kind. Timestamps take RFC 3339 as in runtime-go, or epoch nanoseconds.

use super::window::{Comparison, Predicate};
use super::{Error, Kind, Resource, Result, Value};

const OPERATORS: [(&str, Comparison); 6] = [
    (">=", Comparison::Ge),
    ("<=", Comparison::Le),
    ("!=", Comparison::Ne),
    ("=", Comparison::Eq),
    (">", Comparison::Gt),
    ("<", Comparison::Lt),
];

/// Parses and types `expr` against `res`. Empty means no predicates.
pub(crate) fn predicates(res: &Resource, expr: &str) -> Result<Vec<Predicate>> {
    let expr = expr.trim();
    if expr.is_empty() {
        return Ok(Vec::new());
    }
    if bare_word(expr, "OR") {
        return Err(invalid(format!(
            "filter {expr:?} uses OR; only conjunctions of `column op value` joined by AND are accepted"
        )));
    }
    split_and(expr)
        .iter()
        .map(|clause| {
            let (column, comparison, raw) = clause_parts(clause)?;
            let col = res.column(&column)?;
            let value = coerce(col.kind, &raw).ok_or_else(|| {
                invalid(format!("filter on {column}: {raw:?} is not {:?}", col.kind))
            })?;
            Ok(Predicate {
                column,
                comparison,
                value,
            })
        })
        .collect()
}

fn invalid(message: String) -> Error {
    Error::Invalid(message)
}

/// Positions outside quotes, in order.
fn unquoted(chars: &[char]) -> Vec<bool> {
    let mut quote = None;
    chars
        .iter()
        .map(|&c| match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
                false
            }
            None if c == '"' || c == '\'' => {
                quote = Some(c);
                false
            }
            None => true,
        })
        .collect()
}

fn word_at(chars: &[char], i: usize, word: &str) -> bool {
    let n = word.len();
    i + n <= chars.len()
        && chars[i..i + n]
            .iter()
            .collect::<String>()
            .eq_ignore_ascii_case(word)
        && (i == 0 || chars[i - 1] == ' ')
        && (i + n == chars.len() || chars[i + n] == ' ')
}

fn bare_word(expr: &str, word: &str) -> bool {
    let chars: Vec<char> = expr.chars().collect();
    let free = unquoted(&chars);
    (0..chars.len()).any(|i| free[i] && word_at(&chars, i, word))
}

fn split_and(expr: &str) -> Vec<String> {
    let chars: Vec<char> = expr.chars().collect();
    let free = unquoted(&chars);
    let (mut out, mut current, mut i) = (Vec::new(), String::new(), 0);
    while i < chars.len() {
        if free[i] && word_at(&chars, i, "AND") {
            out.push(std::mem::take(&mut current));
            i += 3;
        } else {
            current.push(chars[i]);
            i += 1;
        }
    }
    out.push(current);
    out.into_iter()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect()
}

/// The earliest operator outside quotes, longest match first.
fn clause_parts(clause: &str) -> Result<(String, Comparison, String)> {
    let chars: Vec<char> = clause.chars().collect();
    let free = unquoted(&chars);
    for i in 1..chars.len() {
        if !free[i] {
            continue;
        }
        let rest: String = chars[i..].iter().collect();
        let Some((op, comparison)) = OPERATORS.iter().find(|(op, _)| rest.starts_with(op)) else {
            continue;
        };
        let column = chars[..i].iter().collect::<String>().trim().to_owned();
        let raw = rest[op.len()..].trim().to_owned();
        if column.is_empty() || raw.is_empty() {
            return Err(invalid(format!("filter clause {clause:?} is incomplete")));
        }
        return Ok((column, *comparison, unquote(&raw).to_owned()));
    }
    Err(invalid(format!(
        "filter clause {clause:?} is not `column op value` with = != > >= < <="
    )))
}

fn unquote(s: &str) -> &str {
    let quoted = s.len() >= 2
        && ((s.starts_with('"') && s.ends_with('"')) || (s.starts_with('\'') && s.ends_with('\'')));
    if quoted { &s[1..s.len() - 1] } else { s }
}

fn coerce(kind: Kind, raw: &str) -> Option<Value> {
    Some(match kind {
        Kind::String => raw.into(),
        Kind::Int => raw.parse::<i64>().ok()?.into(),
        Kind::Enum => raw.parse::<i32>().ok()?.into(),
        Kind::Uint => raw.parse::<u64>().ok()?.into(),
        Kind::Float => raw.parse::<f64>().ok().filter(|f| f.is_finite())?.into(),
        Kind::Bool => raw.parse::<bool>().ok()?.into(),
        Kind::Timestamp => match raw.parse::<i64>() {
            Ok(ns) => ns.into(),
            Err(_) => chrono::DateTime::parse_from_rfc3339(raw)
                .ok()?
                .timestamp_nanos_opt()?
                .into(),
        },
    })
}
