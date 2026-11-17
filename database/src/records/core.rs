//! What every driver shares: the AIP-160 filter subset, offset page tokens, and the columns
//! a driver fills itself. Go's `database/core`.

use chrono::Utc;

use super::{Generated, Resource, Row, Value};
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

/// The page size a backend uses when none is requested.
pub const DEFAULT_PAGE_SIZE: i64 = 50;

/// The total a backend reports when it skipped counting.
pub const NO_TOTAL: i64 = -1;

/// The requested page size, or the default.
pub fn page_size(requested: i32) -> i64 {
    if requested <= 0 {
        DEFAULT_PAGE_SIZE
    } else {
        i64::from(requested)
    }
}

/// The offset a token encodes; 0 for an empty or unreadable one. Go's `core.DecodeToken`.
pub fn decode_token(token: &str) -> i64 {
    token.parse::<i64>().ok().filter(|n| *n >= 0).unwrap_or(0)
}

/// The token after a page of `returned` rows at `offset`; empty once `total` is reached.
pub fn encode_token(offset: i64, returned: i64, total: i64) -> String {
    let next = offset + returned;
    if next >= total {
        String::new()
    } else {
        next.to_string()
    }
}

/// How many rows to read: one past the page when the total is skipped, to learn whether
/// another page exists.
pub fn fetch_limit(limit: i64, omit_total: bool) -> i64 {
    if omit_total { limit + 1 } else { limit }
}

/// The page out of `rows`, and the next token. Go's `core.TrimPage`.
pub fn trim_page<T>(
    mut rows: Vec<T>,
    offset: i64,
    limit: i64,
    total: i64,
    omit_total: bool,
) -> (Vec<T>, String) {
    if !omit_total {
        let token = encode_token(offset, rows.len() as i64, total);
        return (rows, token);
    }
    if rows.len() as i64 > limit {
        rows.truncate(limit as usize);
        return (rows, (offset + limit).to_string());
    }
    (rows, String::new())
}

/// Fills the columns a driver supplies: on create a generated key left empty and both audit
/// times, on update the update time. Go's `core.FillManaged`.
pub fn fill_managed(res: &Resource, cols: &mut Row, on_create: bool) {
    let now = Value::Timestamp(Utc::now());
    for c in &res.columns {
        let empty = cols.get(&c.name).is_none_or(Value::is_empty);
        match c.generated {
            Some(strategy) if on_create && empty => {
                cols.insert(c.name.clone(), Value::String(generate_id(strategy)));
            }
            _ if on_create && (c.auto_create || c.auto_update) => {
                cols.insert(c.name.clone(), now.clone());
            }
            _ if !on_create && c.auto_update => {
                cols.insert(c.name.clone(), now.clone());
            }
            _ => {}
        }
    }
}

/// Whether any column is filled by the driver.
pub fn has_managed(res: &Resource) -> bool {
    res.columns.iter().any(|c| c.managed())
}

/// A new identifier. Go's `core.GenerateID`.
pub fn generate_id(strategy: Generated) -> String {
    match strategy {
        Generated::Uuid => uuid::Uuid::new_v4().to_string(),
        Generated::Ulid => ulid::Ulid::generate().to_string(),
    }
}
