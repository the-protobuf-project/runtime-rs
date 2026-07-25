//! Port of `runtime-go/network/graphql/predicate_test.go`.

use network_graphql::{
    after, and, not, relation, ColumnPatch, Int64, Int64Field, ListRequest, Nullable, Predicate,
    StringField,
};

fn to_json(v: impl serde::Serialize) -> String {
    serde_json::to_string(&v).unwrap()
}

#[test]
fn predicate_single_column() {
    let id = StringField { col: "id" };
    assert_eq!(to_json(id.eq("x")), r#"{"id":{"_eq":"x"}}"#);
    assert_eq!(to_json(id.is_in(["a", "b"])), r#"{"id":{"_in":["a","b"]}}"#);

    let count = Int64Field { col: "memberCount" };
    // Int64 marshals as a quoted string, not a bare number.
    assert_eq!(to_json(count.gt(Int64(1))), r#"{"memberCount":{"_gt":"1"}}"#);
}

#[test]
fn predicate_combinators() {
    let id = StringField { col: "id" };
    let name = StringField { col: "name" };
    let got = to_json(and(&[id.eq("x"), name.like("Bob%")]));
    assert_eq!(got, r#"{"_and":[{"id":{"_eq":"x"}},{"name":{"_like":"Bob%"}}]}"#);

    assert_eq!(to_json(not(id.eq("x"))), r#"{"_not":{"id":{"_eq":"x"}}}"#);

    assert!(Predicate::default().is_omitted());
    assert!(!id.eq("x").is_omitted());
}

#[test]
fn relation_nests_predicate_under_relationship_field() {
    let email = StringField { col: "email" };
    let got = to_json(relation("organisationMembers", email.eq("a@b.com")));
    assert_eq!(got, r#"{"organisationMembers":{"email":{"_eq":"a@b.com"}}}"#);
}

#[test]
fn order_term_json_shape() {
    let display_name = StringField { col: "displayName" };
    assert_eq!(to_json(display_name.desc()), r#"{"displayName":"Desc"}"#);

    let a = StringField { col: "a" };
    let b = StringField { col: "b" };
    assert_eq!(to_json(vec![a.asc(), b.desc()]), r#"[{"a":"Asc"},{"b":"Desc"}]"#);
}

/// Stand-in for a hand-written `ColumnPatch` impl a generated update-input struct would
/// provide, mixing a `Nullable<T>` field with a plain field.
struct Patch {
    display_name: Nullable<String>,
    description: Nullable<String>,
    member_count: i64,
}

impl ColumnPatch for Patch {
    fn set_columns(&self) -> serde_json::Map<String, serde_json::Value> {
        let mut out = serde_json::Map::new();
        if let Some(v) = self.display_name.to_set_entry() {
            out.insert("displayName".into(), v);
        }
        if let Some(v) = self.description.to_set_entry() {
            out.insert("description".into(), v);
        }
        if self.member_count != 0 {
            out.insert(
                "memberCount".into(),
                serde_json::json!({ "set": self.member_count }),
            );
        }
        out
    }
}

#[test]
fn set_columns_distinguishes_unset_null_and_value() {
    let patch = Patch {
        display_name: Nullable::value("Bob".to_string()),
        description: Nullable::null(),
        member_count: 0,
    };
    let got = to_json(patch.set_columns());
    assert_eq!(
        got,
        r#"{"description":{"set":null},"displayName":{"set":"Bob"}}"#
    );
}

#[test]
fn set_columns_zero_value_is_still_emitted() {
    let patch = Patch {
        display_name: Nullable::value(String::new()),
        description: Nullable::unset(),
        member_count: 0,
    };
    assert_eq!(to_json(patch.set_columns()), r#"{"displayName":{"set":""}}"#);
}

#[test]
fn set_columns_all_unset_produces_no_columns() {
    let patch = Patch {
        display_name: Nullable::unset(),
        description: Nullable::unset(),
        member_count: 0,
    };
    assert_eq!(to_json(patch.set_columns()), "{}");
}

#[test]
fn keyset_after_ascending_uses_gt_descending_uses_lt() {
    let created = StringField { col: "createTime" };
    assert_eq!(
        to_json(after(&created.asc(), "2026-01-01")),
        r#"{"createTime":{"_gt":"2026-01-01"}}"#
    );
    assert_eq!(
        to_json(after(&created.desc(), "2026-01-01")),
        r#"{"createTime":{"_lt":"2026-01-01"}}"#
    );
}

#[test]
fn keyset_after_sets_order_and_cursor_on_fresh_request() {
    let created = StringField { col: "createTime" };
    let mut r = ListRequest::default();
    r.keyset_after(created.asc(), "2026-01-01");
    assert_eq!(
        to_json(r.get_where()),
        r#"{"createTime":{"_gt":"2026-01-01"}}"#
    );
    assert_eq!(to_json(r.get_order_by()), r#"[{"createTime":"Asc"}]"#);
}

#[test]
fn keyset_after_composes_with_existing_where_via_and() {
    let created = StringField { col: "createTime" };
    let tenant = StringField { col: "tenant" };
    let mut r = ListRequest::default();
    r.where_(tenant.eq("t1"));
    r.keyset_after(created.asc(), "x");
    let want = r#"{"_and":[{"tenant":{"_eq":"t1"}},{"createTime":{"_gt":"x"}}]}"#;
    assert_eq!(to_json(r.get_where()), want);
}
