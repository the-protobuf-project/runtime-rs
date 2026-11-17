//! Filters refused by `records::core::filter::parse`, in the words of Go's `filterx`: each
//! error names the filter, the 1-based position of what it refuses, and why.

#[path = "support/records.rs"]
mod support;

use database::records::Resource;
use database::records::core::{filter, parse_filter};

fn refused(res: &Resource, text: &str, at: usize, says: &str) {
    let err = filter::parse(res, text).expect_err(text).to_string();
    let pos = format!("(at position {at})");
    assert!(
        err.contains(pos.as_str()) && err.contains(says),
        "{text}: {err}"
    );
    assert!(
        err.contains(&format!("{text:?}")),
        "names the filter: {err}"
    );
}

#[test]
fn refusals_name_the_position() {
    let res = support::books();
    for (text, at, says) in [
        ("nosuch = 1", 1, "cannot filter by \"nosuch\""),
        (r#"author.name = "x""#, 1, "cannot filter by"),
        ("pages = many", 9, "\"many\" is not an integer"),
        (r#"title < "F""#, 1, "unsupported operator for \"title\""),
        ("status > 1", 1, "unsupported operator"),
        ("in_print <= true", 1, "unsupported operator"),
        ("pages : 3", 1, "unsupported operator"),
        ("pages : x", 9, "is not an integer"),
        ("create_time != 2026-01-01", 1, "unsupported operator"),
        (
            "create_time > soon",
            15,
            "is not a timestamp (want RFC 3339)",
        ),
        (
            "create_time > 2026-01-01T00:00:00Z",
            28,
            "unexpected operator \":\"",
        ),
        (r#"cover < "a""#, 1, "unsupported operator"),
        ("status = NOPE", 10, "is not a value of enum test.v1.Status"),
        ("pages >", 7, "operator \">\" is missing a value"),
        ("pages = (1)", 7, "is missing a value"),
        ("(pages > 1", 1, "unbalanced \"(\""),
        ("pages > 1)", 10, "unbalanced \")\""),
        ("( )", 1, "empty parentheses"),
        (r#"title = "Dune"#, 9, "unterminated quoted string"),
        ("pages ! 1", 7, "did you mean !=?"),
        ("pages = 1 OR", 13, "OR is missing its right operand"),
        ("pages = 1 OR AND x", 14, "OR is missing its right operand"),
        ("NOT", 4, "NOT is missing its operand"),
        ("- pages > 1", 1, "\"-\" must prefix a term"),
        ("= 20", 1, "unexpected operator \"=\""),
    ] {
        refused(&res, text, at, says);
    }
}

#[test]
fn nesting_stops_at_go_s_depth() {
    let res = support::books();
    let n = filter::MAX_DEPTH;
    let parens = |k: usize| format!("{}pages > 1{}", "(".repeat(k), ")".repeat(k));
    let nots = |k: usize| format!("{}pages > 1", "NOT ".repeat(k));
    assert!(filter::parse(&res, &parens(n)).is_ok());
    assert!(filter::parse(&res, &nots(n)).is_ok());
    refused(&res, &parens(n + 1), n + 1, "filter nests deeper than 32");
    refused(&res, &nots(n + 1), 4 * n + 1, "filter nests deeper than 32");
    let mixed = format!("{}{}", "-(".repeat(17), "x");
    assert!(
        filter::parse(&res, &mixed)
            .unwrap_err()
            .to_string()
            .contains("deeper than 32")
    );
}

#[test]
fn free_text_needs_search_columns() {
    let mut res = support::books();
    res.search.clear();
    refused(
        &res,
        "pages > 1 dune",
        11,
        "free-text search is not supported here",
    );
}

/// The flat conjunction reads the same grammar, and refuses what a flat list cannot hold.
#[test]
fn a_flat_conjunction_refuses_or_not_and_free_text() {
    for (text, says) in [
        ("a = 1 OR b = 2", "OR"),
        ("NOT a = 1", "NOT"),
        ("dune", "free-text"),
    ] {
        let err = parse_filter(text).expect_err(text).to_string();
        assert!(
            err.contains(says) && err.contains("filter::parse"),
            "{text}: {err}"
        );
    }
    let clauses = parse_filter(r#"a = "x y" b != 2 c:z"#).unwrap();
    let ops: Vec<&str> = clauses.iter().map(|c| c.op.as_str()).collect();
    assert_eq!(
        (clauses[0].value.as_str(), ops),
        ("x y", vec!["=", "!=", ":"])
    );
}
