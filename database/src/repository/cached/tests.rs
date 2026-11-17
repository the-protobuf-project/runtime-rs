//! Unit tests: keyspaces, Go's list scopes, JSON and page keys.

use super::lists::{list_parent, list_scope};
use super::*;
use crate::repository::ListInput;

static BOOK: Keyspace = Keyspace {
    resource: "bookstore.v1.Book",
    model: "Book",
    pattern: "authors/{author}/books/{book}",
    namespace: &[
        Part::Literal("bookstore.v1.book"),
        Part::Literal("author"),
        Part::Segment("author"),
    ],
};
static AUTHOR: Keyspace = Keyspace {
    resource: "bookstore.v1.Author",
    model: "Author",
    pattern: "authors/{author}",
    namespace: &[Part::Literal("bookstore.v1.author")],
};

#[test]
fn namespace_binds_the_parent_segment() {
    assert_eq!(
        BOOK.namespace("authors/ursula/books/lhd").unwrap(),
        "bookstore.v1.book.author.ursula"
    );
    assert!(BOOK.namespace("authors/ursula").is_err());
    assert!(BOOK.namespace("authors//books/lhd").is_err());
    assert!(BOOK.namespace("authors/a:b/books/lhd").is_err());
    assert_eq!(AUTHOR.fixed().as_deref(), Some("bookstore.v1.author"));
    assert_eq!(BOOK.fixed(), None);
}

#[test]
fn edge_values_resolve_to_names() {
    assert_eq!(
        AUTHOR.resolve("authors/ursula").as_deref(),
        Some("authors/ursula")
    );
    assert_eq!(AUTHOR.resolve("ursula").as_deref(), Some("authors/ursula"));
    assert_eq!(BOOK.resolve("lhd"), None);
}

#[test]
fn list_parents_and_scopes_are_gos() {
    let c: &[&str] = &["authors", "books"];
    assert_eq!(
        list_parent("authors/a/books/b", c).as_deref(),
        Some("authors/a")
    );
    assert_eq!(list_parent("shelves/s", c), None);
    assert_eq!(list_parent("shelves/s", &["shelves"]).as_deref(), Some(""));
    assert_eq!(list_scope("authors/-"), "*");
    assert_eq!(list_scope("authors/a"), "authors/a");
    assert_eq!(list_scope(""), "");
}

#[test]
fn json_strings_are_gos() {
    let input = "a<b>&\"\\\n\u{1}\u{2028}\u{e9}\u{8}\u{c}";
    let go = "\"a\\u003cb\\u003e\\u0026\\\"\\\\\\n\\u0001\\u2028\u{e9}\\b\\f\"";
    assert_eq!(go_json_string(input), go);
}

/// Pinned against Go: `json.Marshal(struct{Parent string; In repox.ListInput}{...})`
/// hashed with SHA-256, for the input below (store v1.6.9 to v1.7.1's `repox`).
#[test]
fn page_key_is_gos() {
    let input = ListInput {
        page_size: 25,
        page_token: "tok".into(),
        order_by: "title desc".into(),
        filter: r#"year > 1960 && title = "<a&b>""#.into(),
        show_deleted: true,
        parent: String::new(),
    };
    assert_eq!(page_key("0", &input), format!("page:0:{GO_DIGEST}"));
    let nested = ListInput {
        parent: "authors/ursula".into(),
        ..input
    };
    assert_eq!(page_key("v", &nested), format!("page:v:{GO_DIGEST_NESTED}"));
}

const GO_DIGEST: &str = "19f5aee9413073cd6690535d727b5f7a3db4d558b4d1237b404c87f8aacd2f11";
/// The same request under `List(ctx, "authors/ursula", in)`.
const GO_DIGEST_NESTED: &str = "686cb15fa1d7c1f6683a0b1e63798a3e5ec210507fb774397f8aad5afe94e89a";
