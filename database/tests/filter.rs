//! AIP-160 filters parsed into `records::core::filter::Expr` as Go's `filterx` reads them:
//! precedence, keyword casing, negation, quoting, free text, typing by column kind, and the
//! conformance table over the in-memory reference. Refusals are in `filter_errors`.

#[path = "support/filter_conformance.rs"]
mod conformance;
#[path = "support/records.rs"]
mod support;

use chrono::{TimeZone, Utc};
use database::records::core::filter::{self, Comparison, Contains, Expr, Op};
use database::records::{Kind, Value, core, message_to_columns};

fn parse(text: &str) -> Expr {
    filter::parse(&support::books(), text)
        .unwrap_or_else(|e| panic!("{text}: {e}"))
        .expect("not empty")
}

fn cmp(column: &str, kind: Kind, op: Op, value: Value) -> Expr {
    let column = column.into();
    Expr::Compare(Comparison {
        column,
        kind,
        op,
        value,
    })
}

fn pages(op: Op, n: i64) -> Expr {
    cmp("pages", Kind::Int, op, Value::Int(n))
}

fn not(e: Expr) -> Expr {
    Expr::Not(Box::new(e))
}

fn has(column: &str, value: &str) -> Expr {
    let (column, value) = (column.into(), value.into());
    Expr::Contains(Contains { column, value })
}

#[test]
fn not_binds_tightest_and_or_binds_tighter_than_and() {
    let (p1, p2, p3) = (pages(Op::Eq, 1), pages(Op::Eq, 2), pages(Op::Eq, 3));
    let or23 = Expr::Or(vec![p2.clone(), p3.clone()]);
    let and = |v: Vec<Expr>| Expr::And(v);
    assert_eq!(
        parse("pages = 1 AND pages = 2 OR pages = 3"),
        and(vec![p1.clone(), or23.clone()])
    );
    assert_eq!(
        parse("pages = 1 pages = 2 OR pages = 3"),
        and(vec![p1.clone(), or23])
    );
    assert_eq!(
        parse("(pages = 1 AND pages = 2) OR pages = 3"),
        Expr::Or(vec![and(vec![p1.clone(), p2.clone()]), p3.clone()])
    );
    assert_eq!(
        parse("NOT pages = 1 OR pages = 2"),
        Expr::Or(vec![not(p1.clone()), p2.clone()])
    );
    assert_eq!(
        parse("-pages = 1 and pages = 2"),
        and(vec![not(p1.clone()), p2.clone()])
    );
    assert_eq!(
        parse("-(pages = 1 OR pages = 2)"),
        not(Expr::Or(vec![p1.clone(), p2.clone()]))
    );
    assert_eq!(parse("--pages = 1"), not(not(p1.clone())));
    // A parenthesized group joins the conjunction around it; stray ANDs are skipped.
    assert_eq!(
        parse("AND pages = 1 AND (pages = 2 pages = 3) AND"),
        and(vec![p1.clone(), p2, p3])
    );
    assert_eq!(parse("((pages = 1))"), p1.clone());
    assert_eq!(filter::parse(&support::books(), " AND and ").unwrap(), None);
}

#[test]
fn or_and_not_are_keywords_only_in_capitals() {
    let words = |w: &[&str]| -> Vec<Expr> {
        let any = |t: &str| Expr::Or(vec![has("subtitle", t), has("title", t)]);
        w.iter().map(|t| any(t)).collect()
    };
    assert_eq!(
        parse("dune or emma"),
        Expr::And(words(&["dune", "or", "emma"]))
    );
    assert_eq!(parse("not dune"), Expr::And(words(&["not", "dune"])));
    assert_eq!(parse(r#""NOT" dune"#), Expr::And(words(&["NOT", "dune"])));
    assert_eq!(parse("SUMMER25"), words(&["SUMMER25"]).remove(0));
}

#[test]
fn values_are_typed_by_their_column() {
    let at = Utc.with_ymd_and_hms(2025, 12, 31, 22, 0, 0).unwrap();
    let midnight = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    for (text, want) in [
        ("pages>=-5", pages(Op::Ge, -5)),
        ("isbn != 7", cmp("isbn", Kind::Uint, Op::Ne, Value::Uint(7))),
        (
            "price < 2.5",
            cmp("price", Kind::Float, Op::Lt, Value::Float(2.5)),
        ),
        (
            "in_print = T",
            cmp("in_print", Kind::Bool, Op::Eq, Value::Bool(true)),
        ),
        (
            "status = 2",
            cmp("status", Kind::Enum, Op::Eq, Value::Int(2)),
        ),
        (
            "status != draft",
            cmp("status", Kind::Enum, Op::Ne, Value::Int(1)),
        ),
        (
            "status = STATUS_PUBLISHED",
            cmp("status", Kind::Enum, Op::Eq, Value::Int(2)),
        ),
        (
            r#"cover = "png""#,
            cmp("cover", Kind::Bytes, Op::Eq, Value::Bytes(b"png".to_vec())),
        ),
        (
            r#"create_time > "2026-01-01T00:00:00+02:00""#,
            cmp("create_time", Kind::Timestamp, Op::Gt, Value::Timestamp(at)),
        ),
        (
            "create_time <= 2026-01-01",
            cmp(
                "create_time",
                Kind::Timestamp,
                Op::Le,
                Value::Timestamp(midnight),
            ),
        ),
        ("title:Dune", has("title", "Dune")),
        (r#"subtitle : "a b""#, has("subtitle", "a b")),
        (
            "subtitle = null",
            cmp(
                "subtitle",
                Kind::String,
                Op::Eq,
                Value::String("null".into()),
            ),
        ),
    ] {
        assert_eq!(parse(text), want, "{text}");
    }
}

#[test]
fn strings_are_double_quoted_without_escapes() {
    let title = |s: &str| cmp("title", Kind::String, Op::Eq, Value::String(s.into()));
    for (text, want) in [
        (r#"title = "x AND y""#, "x AND y"),
        (r#"title = "a != (b)""#, "a != (b)"),
        (r#"title = "a\b""#, r"a\b"),
        (r#"title = """#, ""),
        ("title = 'Dune'", "'Dune'"),
        ("title = Dune*", "Dune*"),
        ("title = 2026-01-01", "2026-01-01"),
        ("title = AND", "AND"),
        (r#""title" = Dune"#, "Dune"),
    ] {
        assert_eq!(parse(text), title(want), "{text}");
    }
}

/// The reference: the table over rows in memory, as the Tauri store evaluates it.
#[test]
fn the_in_memory_reference_agrees_with_the_conformance_table() {
    let res = support::books();
    let rows: Vec<_> = conformance::seed(support::book)
        .iter()
        .map(|m| {
            let mut row = message_to_columns(&res, m).unwrap();
            core::fill_managed(&res, &mut row, true);
            (conformance::id(m), row)
        })
        .collect();
    for (text, want) in conformance::CASES {
        let e = filter::parse(&res, text).unwrap().unwrap();
        let got: Vec<&str> = rows
            .iter()
            .filter(|(_, r)| e.matches(r))
            .map(|(id, _)| id.as_str())
            .collect();
        assert_eq!(got, *want, "{text}");
    }
    for text in conformance::REFUSED {
        assert!(filter::parse(&res, text).is_err(), "{text}");
    }
}
