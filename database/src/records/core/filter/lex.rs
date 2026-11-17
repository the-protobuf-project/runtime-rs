//! The tokens of a filter, as protoc-gen-store's Go `filterx.tokenize` splits them, each
//! with the 1-based character position it starts at so an error can point at it.
//!
//! Only a space or a tab separates tokens. A value is a bareword or a double-quoted string;
//! a string has no escapes and ends at the next `"`, and `'` is an ordinary character.

use super::refuse;
use crate::Result;

/// One token.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Tok {
    /// A bareword or a quoted string. `negates` marks a lone `-` written right before a
    /// quoted string or a parenthesis, which it negates: `-"two words"`, `-(a OR b)`.
    Value {
        text: String,
        quoted: bool,
        negates: bool,
    },
    /// `=`, `!=`, `:`, `<`, `<=`, `>` or `>=`.
    Op(&'static str),
    /// `(` or `)`.
    Paren(char),
}

/// A token and the 1-based character position it starts at.
#[derive(Clone, Debug)]
pub(super) struct Token {
    pub tok: Tok,
    pub at: usize,
}

impl Token {
    /// The token as written, for errors.
    pub fn text(&self) -> String {
        match &self.tok {
            Tok::Value { text, .. } => text.clone(),
            Tok::Op(op) => (*op).to_owned(),
            Tok::Paren(c) => c.to_string(),
        }
    }
}

fn delim(c: char) -> bool {
    matches!(
        c,
        ' ' | '\t' | '=' | ':' | '!' | '"' | '<' | '>' | '(' | ')'
    )
}

/// The tokens of `src`. Go's `tokenize`.
pub(super) fn lex(src: &str) -> Result<Vec<Token>> {
    let chars: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let (c, at) = (chars[i], i + 1);
        let tok = match c {
            ' ' | '\t' => {
                i += 1;
                continue;
            }
            '"' => {
                let Some(len) = chars[i + 1..].iter().position(|&c| c == '"') else {
                    return Err(refuse(src, at, "unterminated quoted string in filter"));
                };
                let text = chars[i + 1..i + 1 + len].iter().collect();
                i += len + 2;
                Tok::Value {
                    text,
                    quoted: true,
                    negates: false,
                }
            }
            '=' | ':' => {
                i += 1;
                Tok::Op(if c == '=' { "=" } else { ":" })
            }
            '(' | ')' => {
                i += 1;
                Tok::Paren(c)
            }
            '!' if chars.get(i + 1) == Some(&'=') => {
                i += 2;
                Tok::Op("!=")
            }
            '!' => {
                let what = "unexpected \"!\" in filter (did you mean !=?)";
                return Err(refuse(src, at, what));
            }
            '<' | '>' => {
                let eq = chars.get(i + 1) == Some(&'=');
                i += 1 + usize::from(eq);
                Tok::Op(match (c, eq) {
                    ('<', true) => "<=",
                    ('<', false) => "<",
                    ('>', true) => ">=",
                    _ => ">",
                })
            }
            _ => {
                let start = i;
                while i < chars.len() && !delim(chars[i]) {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                let negates = text == "-" && matches!(chars.get(i), Some('"' | '('));
                Tok::Value {
                    text,
                    quoted: false,
                    negates,
                }
            }
        };
        out.push(Token { tok, at });
    }
    Ok(out)
}
