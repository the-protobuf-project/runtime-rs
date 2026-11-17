//! One restriction, `field comparator value`, and the forms AIP-160 allows there that no
//! provider here evaluates: function calls, traversal into messages, bare values.

use super::lex::{Tok, Token};
use super::refuse;
use super::syntax::{Arg, Node, Parser, Restriction};
use crate::Result;

impl Parser<'_> {
    /// The restriction starting with `field`.
    pub(super) fn restriction(&mut self, field: Token) -> Result<Node> {
        let name = match field.tok {
            Tok::Text(name) => name,
            Tok::Str { text, .. } => {
                if self.peek().is_some_and(|t| matches!(t.tok, Tok::Cmp(_))) {
                    return Err(refuse(self.src, field.at, "a field name is not quoted"));
                }
                return Err(bare(self.src, field.at, &format!("{text:?}")));
            }
            tok => return Err(self.unexpected(Token { tok, ..field })),
        };
        self.no_call(&name, field.end)?;
        if name.contains('.') {
            let what = format!("`{name}` traverses into a message, and columns here are flat");
            return Err(refuse(self.src, field.at, what));
        }
        let (op, at) = match self.peek() {
            Some(&Token {
                tok: Tok::Cmp(op),
                at,
                ..
            }) => (op, at),
            _ => return Err(bare(self.src, field.at, &name)),
        };
        self.pos += 1;
        let Some(value) = self.next() else {
            return Err(refuse(
                self.src,
                at,
                format!("`{op}` needs a value after it"),
            ));
        };
        let arg = match value.tok {
            Tok::Text(text) => {
                self.no_call(&text, value.end)?;
                let wild = text.contains('*');
                Arg {
                    text,
                    quoted: false,
                    wild,
                    at: value.at,
                }
            }
            Tok::Str { text, wild } => Arg {
                text,
                quoted: true,
                wild,
                at: value.at,
            },
            Tok::LParen => {
                let what = "a value cannot be a parenthesized expression";
                return Err(refuse(self.src, value.at, what));
            }
            _ => {
                return Err(refuse(
                    self.src,
                    at,
                    format!("`{op}` needs a value after it"),
                ));
            }
        };
        let at = field.at;
        Ok(Node::Restriction(Restriction {
            field: name,
            at,
            op,
            arg,
        }))
    }

    /// Refuses `name(` as a function call, which no provider here evaluates.
    fn no_call(&self, name: &str, end: usize) -> Result<()> {
        match self.peek() {
            Some(t) if t.tok == Tok::LParen && t.at == end => {
                let what =
                    format!("`{name}(...)` is a function call, which filters here do not support");
                Err(refuse(self.src, end - name.chars().count(), what))
            }
            _ => Ok(()),
        }
    }
}

fn bare(src: &str, at: usize, what: &str) -> crate::Error {
    let what = format!(
        "{what} is a bare value (a global search), which filters here do not support; \
         compare a field, as in `title = {what}`"
    );
    refuse(src, at, what)
}
