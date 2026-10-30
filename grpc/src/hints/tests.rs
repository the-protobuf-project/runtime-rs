use super::*;
use prost_reflect::Value;
use tonic_types::ErrorInfo;

const SVC: &str = "/shop.v1.ShopService/";

fn hinter(product: &str) -> Hinter {
    Hinter::new(Options {
        product: product.to_string(),
        pool: Some(crate::annotations::tests::shop()),
        ..Options::default()
    })
}

/// A request of type `name` with string fields set from `fields`, and a nested resource's
/// name from `"<field>.name"`.
fn req(name: &str, fields: &[(&str, &str)]) -> DynamicMessage {
    let pool = crate::annotations::tests::shop();
    let mut message = DynamicMessage::new(
        pool.get_message_by_name(&format!("shop.v1.{name}"))
            .expect("a shop message"),
    );
    for (path, value) in fields {
        match path.split_once('.') {
            Some((outer, inner)) => {
                let fd = message.descriptor().get_field_by_name(outer).unwrap();
                let Kind::Message(sub_type) = fd.kind() else {
                    panic!("{outer} is not a message");
                };
                let mut sub = DynamicMessage::new(sub_type);
                sub.set_field_by_name(inner, Value::String((*value).to_string()));
                message.set_field(&fd, Value::Message(sub));
            }
            None => message.set_field_by_name(path, Value::String((*value).to_string())),
        }
    }
    message
}

#[test]
fn generic_errors_are_rewritten_to_name_the_resource() {
    let h = hinter("shop");
    let cases: Vec<(&str, &str, DynamicMessage, Status, Vec<&str>)> = vec![
        (
            "a bare not found names the resource and how to list",
            "GetShelf",
            req("GetShelfRequest", &[("name", "shelves/ghost")]),
            Status::not_found("Not found."),
            vec![
                r#"Shelf "shelves/ghost" does not exist"#,
                "ListShelves (GET /v1/shelves)",
            ],
        ),
        (
            "a nested resource lists under its parent",
            "GetBook",
            req("GetBookRequest", &[("name", "shelves/s/books/b")]),
            Status::not_found("not found"),
            vec![
                r#"Book "shelves/s/books/b""#,
                "ListBooks (GET /v1/shelves/s/books)",
            ],
        ),
        (
            "a stale etag says to read again",
            "UpdateShelf",
            req("UpdateShelfRequest", &[("shelf.name", "shelves/s")]),
            Status::aborted("conflict: Message: row changed"),
            vec!["changed after you read it", "GetShelf (GET /v1/shelves/s)"],
        ),
        (
            "a taken id says to choose another",
            "CreateBook",
            req(
                "CreateBookRequest",
                &[("parent", "shelves/s"), ("book_id", "b1")],
            ),
            Status::already_exists("already exists: duplicate key"),
            vec![
                r#""shelves/s/books/b1" already exists"#,
                "Choose another id",
                "GetBook (GET /v1/shelves/s/books/b1)",
            ],
        ),
        (
            "undelete of a live record says there is nothing to restore",
            "UndeleteShelf",
            req("UndeleteShelfRequest", &[("name", "shelves/s")]),
            Status::already_exists("shelves/s is not deleted"),
            vec!["is not deleted, so there is nothing to restore"],
        ),
        (
            "a GET of a custom method says to POST",
            "GetShelf",
            req("GetShelfRequest", &[("name", "shelves/s:undelete")]),
            Status::not_found("not found"),
            vec!["custom methods are called with POST", "not a shelf name"],
        ),
        (
            "a camel-cased kind reads as words",
            "GetShelf",
            req("GetShelfRequest", &[("name", "bookCases/c")]),
            Status::not_found("not found"),
            vec![r#"Book case "bookCases/c" does not exist"#],
        ),
        (
            "a name of no known kind still reads",
            "GetShelf",
            req("GetShelfRequest", &[("name", "lamps/l")]),
            Status::not_found("not found"),
            vec![r#"resource "lamps/l" does not exist"#],
        ),
        (
            "an internal error carries no internals",
            "CreateBook",
            req("CreateBookRequest", &[]),
            Status::internal("ERROR: null value in column \"title\""),
            vec![
                "shop failed unexpectedly while handling CreateBook",
                "fault on our side",
                "the time and the call",
            ],
        ),
    ];
    for (name, method, request, status, want) in cases {
        let got = h.rewrite(&format!("{SVC}{method}"), Some(&request), None, status);
        for w in want {
            assert!(
                got.message().contains(w),
                "{name}: {:?} lacks {w:?}",
                got.message()
            );
        }
        for leak in ["duplicate key", "null value", "Message: row"] {
            assert!(
                !got.message().contains(leak),
                "{name}: leaks {:?}",
                got.message()
            );
        }
    }
}

#[test]
fn errors_that_already_say_what_are_left_alone() {
    let h = hinter("shop");
    let request = req("GetBookRequest", &[("name", "shelves/s/books/b")]);
    let got = h.rewrite(
        &format!("{SVC}GetBook"),
        Some(&request),
        None,
        Status::not_found(r#"Shelf "shelves/s" does not exist."#),
    );
    assert_eq!(got.message(), r#"Shelf "shelves/s" does not exist."#);
}

/// A service that described its error with its own reason keeps its message; a generic
/// reason is rewritten, and the hint keeps its details.
#[test]
fn described_errors_keep_their_message_and_hints_keep_details() {
    let h = hinter("");
    let request = req(
        "CreateBookRequest",
        &[("parent", "shelves/s"), ("book_id", "b")],
    );
    let method = format!("{SVC}CreateBook");
    let described = Status::with_error_details_vec(
        Code::AlreadyExists,
        "Book b already exists and was made from another order.",
        vec![ErrorDetail::ErrorInfo(ErrorInfo::new(
            "BOOK_ID_TAKEN",
            "",
            HashMap::new(),
        ))],
    );
    let got = h.rewrite(&method, Some(&request), None, described);
    assert_eq!(
        got.message(),
        "Book b already exists and was made from another order."
    );

    let generic = Status::with_error_details_vec(
        Code::AlreadyExists,
        "already exists",
        vec![ErrorDetail::ErrorInfo(ErrorInfo::new(
            "ALREADY_EXISTS",
            "shop",
            HashMap::new(),
        ))],
    );
    let got = h.rewrite(&method, Some(&request), None, generic);
    assert_eq!(
        got.get_details_error_info().unwrap().reason,
        "ALREADY_EXISTS"
    );
    let resource = got.get_details_resource_info().unwrap();
    assert_eq!(resource.resource_name, "shelves/s/books/b");
    assert_eq!(resource.resource_type, "shop.example.com/Book");
}

#[test]
fn an_internal_error_carries_the_trace_id() {
    let h = hinter("");
    let msg = h
        .rewrite(
            "/x.Y/Z",
            None,
            Some("0102030000000000000000000000000a"),
            Status::unknown("boom"),
        )
        .message()
        .to_string();
    assert!(msg.starts_with("The service failed"), "{msg}");
    assert!(
        msg.contains("trace 0102030000000000000000000000000a"),
        "{msg}"
    );
}

#[test]
fn trace_ids_come_from_traceparent() {
    let mut metadata = tonic::metadata::MetadataMap::new();
    assert_eq!(trace_id(&metadata), None);
    metadata.insert(
        "traceparent",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            .parse()
            .unwrap(),
    );
    assert_eq!(
        trace_id(&metadata).as_deref(),
        Some("4bf92f3577b34da6a3ce929d0e0e4736")
    );
    metadata.insert(
        "traceparent",
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01"
            .parse()
            .unwrap(),
    );
    assert_eq!(trace_id(&metadata), None);
}

#[test]
fn calls_render_with_their_rest_binding() {
    let h = hinter("");
    assert_eq!(
        h.call("UndeleteShelf", &HashMap::from([("name", "shelves/s")])),
        "UndeleteShelf (POST /v1/shelves/s:undelete)"
    );
    assert_eq!(h.call("NoSuchMethod", &HashMap::new()), "NoSuchMethod");
    assert_eq!(
        h.type_of("shelves/s/books/b").as_deref(),
        Some("shop.example.com/Book")
    );
    // The package-level helpers read the global pool, which knows no shop.
    assert_eq!(type_of("shelves/s"), None);
    assert_eq!(call("GetShelf", &HashMap::new()), "GetShelf");
}

#[test]
fn path_variables_fill_from_what_is_known() {
    let vars = HashMap::from([("parent", "shelves/s"), ("name", "")]);
    assert_eq!(
        fill_path("/v1/{parent=shelves/*}/books", &vars),
        "/v1/shelves/s/books"
    );
    assert_eq!(
        fill_path("/v1/{name=shelves/*}", &vars),
        "/v1/{name=shelves/*}"
    );
    assert_eq!(fill_path("/v1/{Bad}/x", &vars), "/v1/{Bad}/x");
}
