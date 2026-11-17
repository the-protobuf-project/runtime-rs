//! The tokens of an AIP-160 filter, each with the 1-based character position it starts at,
//! so every error can point at what it refuses.

use super::refuse;
use crate::Result;

/// One token. Keywords are matched case-insensitively, so runtime-go's `a = 1 and b = 2`
/// still reads as a conjunction.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Tok {
    LParen,
    RParen,
    Comma,
    /// `-` as negation; a `-` that starts a number is part of the number's text.
    Minus,
    And,
    Or,
    Not,
    /// `=`, `!=`, `<`, `<=`, `>`, `>=`, or the has operator `:`.
    Cmp(&'static str),
    /// Unquoted text: a field name, a number, `true`, `null`, `*`.
    Text(String),
    /// A quoted string, escapes resolved; `wild` when it holds an unescaped `*`.
    Str {
        text: String,
        wild: bool,
    },
}

/// A token and where it starts and ends, as 1-based character positions.
#[derive(Clone, Debug)]
pub(super) struct Token {
    pub tok: Tok,
    pub at: usize,
    pub end: usize,
}

const STOP: &[char] = &['(', ')', ',', '<', '>', '=', '!', ':', '"', '\''];

/// The tokens of `src`, or an error naming the position of the first character that cannot
/// start one.
pub(super) fn lex(src: &str) -> Result<Vec<Token>> {
    let chars: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let start = i;
        let tok = match c {
            _ if c.is_whitespace() => {
                i += 1;
                continue;
            }
            '(' | ')' | ',' => {
                i += 1;
                match c {
                    '(' => Tok::LParen,
                    ')' => Tok::RParen,
                    _ => Tok::Comma,
                }
            }
            '<' | '>' | '=' | '!' | ':' => {
                let two = chars.get(i + 1) == Some(&'=');
                i += if two && c != '=' && c != ':' { 2 } else { 1 };
                Tok::Cmp(match (c, i - start) {
                    ('<', 2) => "<=",
                    ('>', 2) => ">=",
                    ('!', 2) => "!=",
                    ('<', _) => "<",
                    ('>', _) => ">",
                    ('=', _) => "=",
                    (':', _) => ":",
                    _ => {
                        return Err(refuse(
                            src,
                            start + 1,
                            "`!` is not an operator; use `!=` or NOT",
                        ));
                    }
                })
            }
            '"' | '\'' => {
                let (text, wild, next) = string(src, &chars, i)?;
                i = next;
                Tok::Str { text, wild }
            }
            '-' if !chars
                .get(i + 1)
                .is_some_and(|n| n.is_ascii_digit() || *n == '.') =>
            {
                i += 1;
                Tok::Minus
            }
            _ => {
                i += 1;
                // Text that starts with a digit is a value, never a field, so a `:` in it is
                // part of an unquoted time (`2026-01-01T00:00:00Z`), not the has operator.
                let numeric = c.is_ascii_digit() || c == '-';
                while i < chars.len()
                    && !chars[i].is_whitespace()
                    && (!STOP.contains(&chars[i]) || (numeric && chars[i] == ':'))
                {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                match text.to_ascii_uppercase().as_str() {
                    "AND" => Tok::And,
                    "OR" => Tok::Or,
                    "NOT" => Tok::Not,
                    _ => Tok::Text(text),
                }
            }
        };
        out.push(Token {
            tok,
            at: start + 1,
            end: i + 1,
        });
    }
    Ok(out)
}

/// The quoted string opening at `open`: its text with escapes resolved, whether it holds an
/// unescaped `*`, and the index past its closing quote.
fn string(src: &str, chars: &[char], open: usize) -> Result<(String, bool, usize)> {
    let quote = chars[open];
    let (mut text, mut wild, mut i) = (String::new(), false, open + 1);
    while i < chars.len() {
        match chars[i] {
            c if c == quote => return Ok((text, wild, i + 1)),
            '\\' => {
                let Some(&e) = chars.get(i + 1) else { break };
                text.push(match e {
                    '\\' | '"' | '\'' | '*' => e,
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    _ => {
                        let what = format!(
                            "`\\{e}` is not an escape; use \\\\ \\\" \\' \\* \\n \\r or \\t"
                        );
                        return Err(refuse(src, i + 1, what));
                    }
                });
                i += 2;
            }
            c => {
                wild |= c == '*';
                text.push(c);
                i += 1;
            }
        }
    }
    Err(refuse(src, open + 1, "this string is never closed"))
}
