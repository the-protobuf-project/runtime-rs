//! AIP-160's grammar over [`lex`]'s tokens, before any column is resolved:
//!
//! ```text
//! expression  = sequence { AND sequence }
//! sequence    = factor { factor }              (juxtaposition is AND)
//! factor      = term { OR term }               (OR binds tighter than AND)
//! term        = [ NOT | - ] simple
//! simple      = "(" expression ")" | restriction
//! restriction = field comparator value
//! ```

use super::lex::{Tok, Token, lex};
use super::refuse;
use crate::Result;

/// A filter as written: the tree of AND, OR and NOT over restrictions.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Node {
    And(Vec<Node>),
    Or(Vec<Node>),
    Not(Box<Node>),
    Restriction(Restriction),
}

/// `field op value`, untyped.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Restriction {
    pub field: String,
    pub at: usize,
    pub op: &'static str,
    pub arg: Arg,
}

/// A restriction's value as written.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Arg {
    pub text: String,
    pub quoted: bool,
    /// Holds an unescaped `*`, which AIP-160 reads as a wildcard.
    pub wild: bool,
    pub at: usize,
}

/// The tree `src` spells, or `None` for an empty filter.
pub(crate) fn parse(src: &str) -> Result<Option<Node>> {
    let toks = lex(src)?;
    if toks.is_empty() {
        return Ok(None);
    }
    let mut p = Parser { src, toks, pos: 0 };
    let node = p.expression()?;
    match p.peek() {
        None => Ok(Some(node)),
        Some(t) => Err(p.unexpected(t.clone())),
    }
}

pub(super) struct Parser<'a> {
    pub(super) src: &'a str,
    toks: Vec<Token>,
    pub(super) pos: usize,
}

impl Parser<'_> {
    pub(super) fn peek(&self) -> Option<&Token> {
        self.toks.get(self.pos)
    }

    pub(super) fn next(&mut self) -> Option<Token> {
        let t = self.toks.get(self.pos).cloned();
        self.pos += 1;
        t
    }

    fn eat(&mut self, tok: &Tok) -> bool {
        let hit = self.peek().is_some_and(|t| t.tok == *tok);
        self.pos += usize::from(hit);
        hit
    }

    /// Where the input ends, for errors about what is missing there.
    fn end(&self) -> usize {
        self.src.chars().count() + 1
    }

    pub(super) fn unexpected(&self, t: Token) -> crate::Error {
        let what = match t.tok {
            Tok::RParen => "`)` closes nothing".to_owned(),
            Tok::Comma => "`,` separates function arguments, which filters here do not take".into(),
            Tok::And | Tok::Or => "AND and OR need an expression on both sides".into(),
            Tok::Cmp(op) => format!("`{op}` needs a field before it"),
            _ => "this does not continue the filter".into(),
        };
        refuse(self.src, t.at, what)
    }

    fn expression(&mut self) -> Result<Node> {
        let mut all = vec![self.sequence()?];
        while self.eat(&Tok::And) {
            all.push(self.sequence()?);
        }
        Ok(join(all, Node::And))
    }

    fn sequence(&mut self) -> Result<Node> {
        let mut all = vec![self.factor()?];
        while self.peek().is_some_and(|t| starts_term(&t.tok)) {
            all.push(self.factor()?);
        }
        Ok(join(all, Node::And))
    }

    fn factor(&mut self) -> Result<Node> {
        let mut any = vec![self.term()?];
        while self.eat(&Tok::Or) {
            any.push(self.term()?);
        }
        Ok(join(any, Node::Or))
    }

    fn term(&mut self) -> Result<Node> {
        if self.eat(&Tok::Not) || self.eat(&Tok::Minus) {
            return Ok(Node::Not(Box::new(self.term()?)));
        }
        match self.next() {
            Some(Token {
                tok: Tok::LParen,
                at,
                ..
            }) => {
                let inner = self.expression()?;
                if !self.eat(&Tok::RParen) {
                    return Err(refuse(self.src, at, "this `(` is never closed"));
                }
                Ok(inner)
            }
            Some(t) => self.restriction(t),
            None => Err(refuse(
                self.src,
                self.end(),
                "the filter ends where an expression should start",
            )),
        }
    }
}

fn starts_term(tok: &Tok) -> bool {
    matches!(
        tok,
        Tok::Text(_) | Tok::Str { .. } | Tok::LParen | Tok::Not | Tok::Minus
    )
}

fn join(mut nodes: Vec<Node>, make: fn(Vec<Node>) -> Node) -> Node {
    if nodes.len() == 1 {
        nodes.remove(0)
    } else {
        make(nodes)
    }
}
