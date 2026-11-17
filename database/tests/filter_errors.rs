//! AIP-160 filters refused by `records::core::filter::parse`: each error names the filter,
//! the 1-based position of what it refuses, and why.

#[path = "support/records.rs"]
mod support;

use database::records::core::{filter, parse_filter};

#[test]
fn refusals_name_the_position() {
    for (text, at, says) in [
        ("nosuch = 1", 1, "not a column"),
        ("pages = many", 9, "a signed integer"),
        ("pages >", 7, "needs a value"),
        ("(pages > 1", 1, "never closed"),
        ("pages > 1)", 10, "closes nothing"),
        (r#"title = "Dune"#, 9, "never closed"),
        (r#"title = "Dune*""#, 9, "wildcard"),
        ("title = Dune*", 9, "wildcard"),
        (r#"author.name = "x""#, 1, "traverses into a message"),
        ("size(title) > 2", 1, "function call"),
        ("pages = max(1)", 9, "function call"),
        ("Dune", 1, "bare value"),
        ("title LIKE Dune", 1, "bare value"),
        (r#""pages" = 1"#, 1, "not quoted"),
        (r#"title:"Dune""#, 7, "has operator"),
        (r#"cover < "a""#, 1, "compares with = and != only"),
        ("subtitle < null", 12, "null compares"),
        ("pages = 1.5s", 9, "durations are not supported"),
        ("pages ! 1", 7, "`!` is not an operator"),
        ("pages = 1 OR", 13, "ends where an expression"),
        ("pages = 1 AND AND pages = 2", 15, "AND and OR need"),
        (r#"title = "a\q""#, 11, "not an escape"),
        ("pages = (1)", 9, "parenthesized"),
        ("= 20", 1, "needs a field"),
        ("pages = 1, pages = 2", 10, "function arguments"),
    ] {
        let err = filter::parse(&support::books(), text)
            .expect_err(text)
            .to_string();
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
    assert_eq!(filter::parse(&support::books(), "  ").unwrap(), None);
}

/// The flat conjunction reads the same grammar, and refuses what a flat list cannot hold.
#[test]
fn a_flat_conjunction_refuses_or_not_and_has() {
    for (text, says) in [
        ("a = 1 OR b = 2", "OR"),
        ("NOT a = 1", "NOT"),
        ("a:*", "has"),
    ] {
        let err = parse_filter(text).expect_err(text).to_string();
        assert!(
            err.contains(says) && err.contains("filter::parse"),
            "{text}: {err}"
        );
    }
    let clauses = parse_filter(r#"a = "x \"y\"" b != 2"#).unwrap();
    assert_eq!(
        (clauses[0].value.as_str(), clauses[1].op.as_str()),
        (r#"x "y""#, "!=")
    );
}
