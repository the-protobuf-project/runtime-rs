//! The innermost terms of Go's `filterx` grammar: a parenthesized group, `field op value`,
//! or a lone value, which is a free-text term.

use super::lex::{Tok, Token};
use super::refuse;
use super::syntax::{MAX_DEPTH, Node, Parser};
use crate::Result;

impl Parser<'_> {
    pub(super) fn simple(&mut self, depth: usize) -> Result<Node> {
        let t = self.toks[self.i].clone();
        let text = match t.tok {
            Tok::Paren('(') => return self.group(depth, t.at),
            Tok::Paren(_) => {
                return Err(refuse(self.src, t.at, "unbalanced \")\" in filter"));
            }
            Tok::Op(op) => {
                let what = format!("unexpected operator {op:?} in filter");
                return Err(refuse(self.src, t.at, what));
            }
            Tok::Value { text, .. } => text,
        };
        let Some(Token {
            tok: Tok::Op(op),
            at,
        }) = self.toks.get(self.i + 1).cloned()
        else {
            self.i += 1;
            return Ok(Node::Search { text, at: t.at });
        };
        let Some(Token {
            tok: Tok::Value { text: value, .. },
            at: value_at,
        }) = self.toks.get(self.i + 2).cloned()
        else {
            let what = format!("filter operator {op:?} is missing a value");
            return Err(refuse(self.src, at, what));
        };
        self.i += 3;
        Ok(Node::Restriction {
            field: text,
            at: t.at,
            op,
            value,
            value_at,
        })
    }

    fn group(&mut self, depth: usize, at: usize) -> Result<Node> {
        if depth >= MAX_DEPTH {
            let what = format!("filter nests deeper than {MAX_DEPTH}");
            return Err(refuse(self.src, at, what));
        }
        self.i += 1;
        let mut all = self.expression(depth + 1)?;
        if !self.at_paren(')') {
            return Err(refuse(self.src, at, "unbalanced \"(\" in filter"));
        }
        self.i += 1;
        match all.len() {
            0 => Err(refuse(self.src, at, "empty parentheses in filter")),
            1 => Ok(all.remove(0)),
            _ => Ok(Node::And(all)),
        }
    }
}
