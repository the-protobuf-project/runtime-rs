//! A filter as a flat conjunction of `column op value` clauses: runtime-go's `core.Clause`
//! and `core.ParseFilter`, for callers that want no more than that. It reads the same
//! grammar as [`super::filter`], so quoting and errors agree; providers use the full tree.

use super::filter::syntax::{self, Node};
use crate::{Error, Result};

/// One `column op value` comparison. Go's `core.Clause`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Clause {
    /// The column, as written.
    pub column: String,
    /// One of `=`, `!=`, `>`, `>=`, `<`, `<=`.
    pub op: String,
    /// The value, unquoted and unescaped.
    pub value: String,
}

/// Parses a conjunction of `column op value` clauses joined by `AND`, untyped and unresolved.
/// A filter that needs more (`OR`, `NOT`, has) is refused, naming
/// [`super::filter::parse`], which takes the whole of AIP-160. Go's `core.ParseFilter`.
pub fn parse_filter(expr: &str) -> Result<Vec<Clause>> {
    let mut out = Vec::new();
    if let Some(node) = syntax::parse(expr)? {
        flatten(expr, node, &mut out)?;
    }
    Ok(out)
}

fn flatten(expr: &str, node: Node, out: &mut Vec<Clause>) -> Result<()> {
    let needs = match node {
        Node::And(all) => {
            return all.into_iter().try_for_each(|n| flatten(expr, n, out));
        }
        Node::Restriction(r) if r.op != ":" => {
            out.push(Clause {
                column: r.field,
                op: r.op.to_owned(),
                value: r.arg.text,
            });
            return Ok(());
        }
        Node::Restriction(_) => "the has operator `:`",
        Node::Or(_) => "OR",
        Node::Not(_) => "NOT",
    };
    Err(Error::Invalid(format!(
        "filter {expr:?} uses {needs}, which a flat conjunction cannot hold; \
         records::core::filter::parse reads all of AIP-160"
    )))
}
