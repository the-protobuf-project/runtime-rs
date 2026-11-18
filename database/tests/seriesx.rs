//! The time-series stores' shared runtime: reductions are named for what they
//! reduce, and stored records decode strictly.
use database::seriesx::{Column, decode, reduce};
use database::store::{Record, Reducer, Value};

#[derive(Clone, Copy)]
struct Temperature;
impl Column for Temperature {
    fn column(self) -> &'static str {
        "temperature_c"
    }
}

#[test]
fn a_reduction_is_named_for_its_function_and_column() {
    for (func, alias) in [
        (Reducer::Avg, "avg_temperature_c"),
        (Reducer::Min, "min_temperature_c"),
        (Reducer::Max, "max_temperature_c"),
        (Reducer::Sum, "sum_temperature_c"),
        (Reducer::Count, "count_temperature_c"),
    ] {
        let r = reduce(func, Temperature);
        assert_eq!(r.column, "temperature_c");
        assert_eq!(r.alias, alias);
    }
}

fn record(entries: &[(&str, Value)]) -> Record {
    entries
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

#[test]
fn a_record_decodes_its_columns_and_consumes_them() {
    let mut r = record(&[
        ("site", Value::String("north".into())),
        ("count", Value::from(7)),
        ("ok", Value::Bool(true)),
    ]);
    assert_eq!(decode::string(&mut r, "site").unwrap(), "north");
    assert_eq!(decode::int::<i32>(&mut r, "count").unwrap(), 7);
    assert!(decode::boolean(&mut r, "ok").unwrap());
    assert!(r.is_empty(), "decoding takes each column out");
}

#[test]
fn a_missing_column_or_the_wrong_kind_is_an_error_not_a_default() {
    let mut r = record(&[("count", Value::String("seven".into()))]);
    assert!(decode::string(&mut r, "site").is_err(), "a missing column");
    assert!(
        decode::int::<i64>(&mut r, "count").is_err(),
        "the wrong kind"
    );
    let mut big = record(&[("n", Value::from(i64::from(i32::MAX) + 1))]);
    assert!(decode::int::<i32>(&mut big, "n").is_err(), "out of range");
}
