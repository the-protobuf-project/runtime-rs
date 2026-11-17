//! The grammar of protoc-gen-store's Go `filterx.Parse`, ported term for term so a Rust
//! service reads a filter as a Go one does:
//!
//! ```text
//! expression = { AND } factor { { AND } factor }   (whitespace is AND; stray ANDs skip)
//! factor     = term { OR term }                    (OR binds tighter than AND)
//! term       = ( NOT | "-" ) term | simple
//! simple     = "(" expression ")" | value op value | value
//! ```
//!
//! `AND` is a keyword in any case, `OR` and `NOT` only in capitals, and none of them when
//! quoted. A value with no operator after it is a free-text search term. Parentheses and
//! negations nest at most [`MAX_DEPTH`] deep.

use super::lex::{Tok, Token, lex};
use super::refuse;
use crate::Result;

/// How deep parentheses and negations nest. Go's `maxFilterDepth`.
pub const MAX_DEPTH: usize = 32;

/// A filter as written.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Node {
    And(Vec<Node>),
    Or(Vec<Node>),
    Not(Box<Node>),
    /// `field op value`, untyped.
    Restriction {
        field: String,
        at: usize,
        op: &'static str,
        value: String,
        value_at: usize,
    },
    /// A value with no operator: a free-text search term.
    Search {
        text: String,
        at: usize,
    },
}

/// The tree `src` spells, or `None` when it holds no term. Go's `Parse`.
pub(crate) fn parse(src: &str) -> Result<Option<Node>> {
    let toks = lex(src)?;
    let mut p = Parser { src, toks, i: 0 };
    let mut all = p.expression(0)?;
    if let Some(t) = p.toks.get(p.i) {
        return Err(refuse(
            src,
            t.at,
            format!("unbalanced {:?} in filter", t.text()),
        ));
    }
    Ok(match all.len() {
        0 => None,
        1 => all.pop(),
        _ => Some(Node::And(all)),
    })
}

pub(super) struct Parser<'a> {
    pub(super) src: &'a str,
    pub(super) toks: Vec<Token>,
    pub(super) i: usize,
}

impl Parser<'_> {
    /// Whether an unquoted `kw` comes next: `AND` in any case, `OR` and `NOT` in capitals.
    pub(super) fn at(&self, kw: &str) -> bool {
        let Some(Tok::Value {
            text,
            quoted: false,
            ..
        }) = self.toks.get(self.i).map(|t| &t.tok)
        else {
            return false;
        };
        text == kw || (kw == "AND" && text.eq_ignore_ascii_case(kw))
    }

    pub(super) fn at_paren(&self, c: char) -> bool {
        matches!(self.toks.get(self.i), Some(Token { tok: Tok::Paren(p), .. }) if *p == c)
    }

    /// Whether the input ends, or a `)`, `AND` or `OR` comes, where an operand should.
    fn no_operand(&self) -> bool {
        self.i >= self.toks.len() || self.at_paren(')') || self.at("AND") || self.at("OR")
    }

    /// Where the next token starts, or the end of the input.
    fn here(&self) -> usize {
        self.toks
            .get(self.i)
            .map_or(self.src.chars().count() + 1, |t| t.at)
    }

    pub(super) fn expression(&mut self, depth: usize) -> Result<Vec<Node>> {
        let mut all = Vec::new();
        loop {
            while self.at("AND") {
                self.i += 1;
            }
            if self.i >= self.toks.len() || self.at_paren(')') {
                return Ok(all);
            }
            match self.factor(depth)? {
                Node::And(group) => all.extend(group),
                node => all.push(node),
            }
        }
    }

    fn factor(&mut self, depth: usize) -> Result<Node> {
        let mut any = vec![self.term(depth)?];
        while self.at("OR") {
            self.i += 1;
            if self.no_operand() {
                let what = "OR is missing its right operand";
                return Err(refuse(self.src, self.here(), what));
            }
            any.push(self.term(depth)?);
        }
        Ok(if any.len() == 1 {
            any.remove(0)
        } else {
            Node::Or(any)
        })
    }

    fn term(&mut self, depth: usize) -> Result<Node> {
        let at = self.here();
        let negate = if self.at("NOT") {
            self.i += 1;
            true
        } else if let Tok::Value {
            text,
            quoted: false,
            negates,
        } = &mut self.toks[self.i].tok
            && text.starts_with('-')
        {
            if text != "-" {
                text.remove(0); // -a
                self.toks[self.i].at += 1;
            } else if *negates {
                self.i += 1; // -"a b", -(a b)
            } else {
                return Err(refuse(self.src, at, "\"-\" must prefix a term"));
            }
            true
        } else {
            false
        };
        if !negate {
            return self.simple(depth);
        }
        if depth >= MAX_DEPTH {
            let what = format!("filter nests deeper than {MAX_DEPTH}");
            return Err(refuse(self.src, at, what));
        }
        if self.no_operand() {
            return Err(refuse(self.src, self.here(), "NOT is missing its operand"));
        }
        Ok(Node::Not(Box::new(self.term(depth + 1)?)))
    }
}
