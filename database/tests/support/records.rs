//! A `Book` resource for the record-store tests: every column kind, an optional field, a
//! unique column and audit timestamps, built from descriptors so no protoc is needed.

#![allow(dead_code)]

use database::records::{Column, Kind, Resource, Value};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, OneofDescriptorProto,
};

fn field(name: &str, number: i32, ty: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(ty as i32),
        json_name: Some(name.into()),
        ..Default::default()
    }
}

/// `test.v1.Book`: `name` (key), `title` (unique), `pages` int32, `price` double, `in_print`
/// bool, `cover` bytes, `status` enum, `subtitle` optional string, `isbn` uint64,
/// `create_time` and `update_time` Timestamps.
pub fn book_descriptor() -> MessageDescriptor {
    static BOOK: std::sync::OnceLock<MessageDescriptor> = std::sync::OnceLock::new();
    BOOK.get_or_init(build_book).clone()
}

fn build_book() -> MessageDescriptor {
    let mut pool = DescriptorPool::new();
    let ts = DescriptorPool::global()
        .get_file_by_name("google/protobuf/timestamp.proto")
        .expect("prost-reflect carries the well-known types");
    pool.add_file_descriptor_proto(ts.file_descriptor_proto().clone())
        .unwrap();
    let message_field = |name: &str, number| FieldDescriptorProto {
        type_name: Some(".google.protobuf.Timestamp".into()),
        ..field(name, number, Type::Message)
    };
    let book = DescriptorProto {
        name: Some("Book".into()),
        field: vec![
            field("name", 1, Type::String),
            field("title", 2, Type::String),
            field("pages", 3, Type::Int32),
            field("price", 4, Type::Double),
            field("in_print", 5, Type::Bool),
            field("cover", 6, Type::Bytes),
            FieldDescriptorProto {
                type_name: Some(".test.v1.Status".into()),
                ..field("status", 7, Type::Enum)
            },
            FieldDescriptorProto {
                oneof_index: Some(0),
                proto3_optional: Some(true),
                ..field("subtitle", 8, Type::String)
            },
            field("isbn", 9, Type::Uint64),
            message_field("create_time", 10),
            message_field("update_time", 11),
        ],
        oneof_decl: vec![OneofDescriptorProto {
            name: Some("_subtitle".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let status = EnumDescriptorProto {
        name: Some("Status".into()),
        value: ["STATUS_UNSPECIFIED", "DRAFT", "PUBLISHED"]
            .iter()
            .enumerate()
            .map(|(i, n)| EnumValueDescriptorProto {
                name: Some((*n).into()),
                number: Some(i as i32),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    pool.add_file_descriptor_proto(FileDescriptorProto {
        name: Some("test/v1/book.proto".into()),
        package: Some("test.v1".into()),
        dependency: vec!["google/protobuf/timestamp.proto".into()],
        message_type: vec![book],
        enum_type: vec![status],
        syntax: Some("proto3".into()),
        ..Default::default()
    })
    .unwrap();
    pool.get_message_by_name("test.v1.Book").unwrap()
}

/// The `Book` resource, in table `books`, keyed by `name`; `subtitle` is nullable, `title`
/// unique, and the two times are audit columns the driver fills.
pub fn books() -> Resource {
    let mut create = Column::new("create_time", Kind::Timestamp).sql_type("TIMESTAMPTZ");
    create.auto_create = true;
    let mut update = Column::new("update_time", Kind::Timestamp).sql_type("TIMESTAMPTZ");
    update.auto_update = true;
    Resource::new(
        book_descriptor(),
        "books",
        "name",
        vec![
            Column::new("name", Kind::String)
                .primary_key()
                .sql_type("VARCHAR(255)"),
            Column::new("title", Kind::String)
                .not_null()
                .unique()
                .sql_type("VARCHAR(255)"),
            Column::new("pages", Kind::Int)
                .not_null()
                .sql_type("INTEGER"),
            Column::new("price", Kind::Float)
                .not_null()
                .sql_type("DOUBLE PRECISION"),
            Column::new("in_print", Kind::Bool)
                .not_null()
                .sql_type("BOOLEAN"),
            Column::new("cover", Kind::Bytes)
                .not_null()
                .sql_type("BYTEA"),
            Column::new("status", Kind::Enum)
                .not_null()
                .sql_type("INTEGER"),
            Column::new("subtitle", Kind::String).sql_type("VARCHAR(255)"),
            Column::new("isbn", Kind::Uint)
                .not_null()
                .sql_type("BIGINT"),
            create,
            update,
        ],
    )
}

/// A book named `books/<id>`, titled `title`, of `pages` pages.
pub fn book(id: &str, title: &str, pages: i32) -> DynamicMessage {
    let mut m = DynamicMessage::new(book_descriptor());
    m.set_field_by_name("name", prost_reflect::Value::String(format!("books/{id}")));
    m.set_field_by_name("title", prost_reflect::Value::String(title.into()));
    m.set_field_by_name("pages", prost_reflect::Value::I32(pages));
    m.set_field_by_name("price", prost_reflect::Value::F64(9.5));
    m.set_field_by_name("in_print", prost_reflect::Value::Bool(true));
    m.set_field_by_name("cover", prost_reflect::Value::Bytes(b"png".to_vec().into()));
    m.set_field_by_name("status", prost_reflect::Value::EnumNumber(2));
    m.set_field_by_name("isbn", prost_reflect::Value::U64(9_780_441_013_593));
    m
}

/// A field of `m`, as a record-store [`Value`] would hold it, for assertions.
pub fn string_field(m: &DynamicMessage, name: &str) -> String {
    m.get_field_by_name(name)
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// Whether `v` is null.
pub fn is_null(v: Option<&Value>) -> bool {
    matches!(v, Some(Value::Null))
}
