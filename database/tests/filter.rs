//! AIP-160 filters parsed into `records::core::filter::Expr`: precedence, parentheses,
//! quoting, typing by column kind, and the conformance table run over the in-memory
//! reference every provider agrees with. Refusals are in `filter_errors`.

#[path = "support/filter_conformance.rs"]
mod conformance;
#[path = "support/records.rs"]
mod support;

use chrono::{TimeZone, Utc};
use database::records::core::filter::{self, Comparison, Expr, Op};
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

#[test]
fn not_binds_tightest_and_or_binds_tighter_than_and() {
    let (p1, p2, p3) = (pages(Op::Eq, 1), pages(Op::Eq, 2), pages(Op::Eq, 3));
    let or23 = Expr::Or(vec![p2.clone(), p3.clone()]);
    assert_eq!(
        parse("pages = 1 AND pages = 2 OR pages = 3"),
        Expr::And(vec![p1.clone(), or23.clone()])
    );
    assert_eq!(
        parse("pages = 1 pages = 2 OR pages = 3"),
        Expr::And(vec![p1.clone(), or23])
    );
    assert_eq!(
        parse("(pages = 1 AND pages = 2) OR pages = 3"),
        Expr::Or(vec![Expr::And(vec![p1.clone(), p2.clone()]), p3.clone()])
    );
    assert_eq!(
        parse("NOT pages = 1 OR pages = 2"),
        Expr::Or(vec![not(p1.clone()), p2.clone()])
    );
    assert_eq!(
        parse("-pages = 1 AND pages = 2"),
        Expr::And(vec![not(p1.clone()), p2.clone()])
    );
    assert_eq!(
        parse("NOT (pages = 1 OR pages = 2)"),
        not(Expr::Or(vec![p1.clone(), p2]))
    );
    assert_eq!(parse("((pages = 1))"), p1);
    assert_eq!(
        parse("pages = 1 and pages = 3 or pages = 1"),
        parse("pages = 1 AND (pages = 3 OR pages = 1)")
    );
}

#[test]
fn values_are_typed_by_their_column() {
    let at = Utc.with_ymd_and_hms(2025, 12, 31, 22, 0, 0).unwrap();
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
            "status <= 2",
            cmp("status", Kind::Enum, Op::Le, Value::Int(2)),
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
            "create_time > 2026-01-01T00:00:00+02:00",
            cmp("create_time", Kind::Timestamp, Op::Gt, Value::Timestamp(at)),
        ),
        ("subtitle:*", Expr::Has("subtitle".into())),
        ("subtitle != null", Expr::Has("subtitle".into())),
        ("subtitle = null", Expr::Null("subtitle".into())),
        (
            r#"subtitle = "null""#,
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
fn strings_quote_and_escape() {
    let title = |s: &str| cmp("title", Kind::String, Op::Eq, Value::String(s.into()));
    for (text, want) in [
        (r#"title = "x AND y""#, "x AND y"),
        (r#"title = 'a != (b)'"#, "a != (b)"),
        (r#"title = "say \"hi\"""#, r#"say "hi""#),
        (r#"title = 'it\'s'"#, "it's"),
        (r#"title = "a\\b\tc\n""#, "a\\b\tc\n"),
        (r#"title = "5\* stars""#, "5* stars"),
        (r#"title = """#, ""),
        ("title = Dune", "Dune"),
        ("title = 2026-01-01", "2026-01-01"),
    ] {
        assert_eq!(parse(text), title(want), "{text}");
    }
}

#[test]
fn negation_reaches_the_leaves() {
    let e = parse(r#"NOT (pages < 3 AND (subtitle:* OR NOT title = "x"))"#);
    let title = cmp("title", Kind::String, Op::Eq, Value::String("x".into()));
    let want = Expr::Or(vec![
        pages(Op::Ge, 3),
        Expr::And(vec![Expr::Null("subtitle".into()), title.clone()]),
    ]);
    assert_eq!(e.without_not(), want);
    assert_eq!(e.negated().negated(), want);
    for (op, flipped) in [(Op::Eq, Op::Ne), (Op::Lt, Op::Ge), (Op::Le, Op::Gt)] {
        assert_eq!((op.negated(), flipped.negated()), (flipped, op));
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
