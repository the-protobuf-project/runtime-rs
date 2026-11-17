//! The SQL-backend fixture, as runtime-go's `orm` tests build it: a `Book` of `id`, `title`,
//! `published_year` and a `genre` enum, the pools each dialect is opened with, and a table
//! per run so engines with a shared database do not collide between tests or runs.

#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

use database::orm::sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use database::records::{Column, DB, Kind, Resource, TxWork};
use futures::future::BoxFuture;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value as P};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto,
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

/// `bookstore.v1.Book`, Go's `bookMD`.
pub fn book_md() -> MessageDescriptor {
    static MD: std::sync::OnceLock<MessageDescriptor> = std::sync::OnceLock::new();
    MD.get_or_init(|| {
        let genre = EnumDescriptorProto {
            name: Some("Genre".into()),
            value: ["GENRE_UNSPECIFIED", "GENRE_FICTION"]
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
        let book = DescriptorProto {
            name: Some("Book".into()),
            field: vec![
                field("id", 1, Type::String),
                field("title", 2, Type::String),
                field("published_year", 3, Type::Int32),
                FieldDescriptorProto {
                    type_name: Some(".bookstore.v1.Genre".into()),
                    ..field("genre", 4, Type::Enum)
                },
            ],
            ..Default::default()
        };
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(FileDescriptorProto {
            name: Some("bookstore/v1/orm_test.proto".into()),
            package: Some("bookstore.v1".into()),
            message_type: vec![book],
            enum_type: vec![genre],
            syntax: Some("proto3".into()),
            ..Default::default()
        })
        .unwrap();
        pool.get_message_by_name("bookstore.v1.Book").unwrap()
    })
    .clone()
}

/// Go's `bookRes`: table `books` keyed by `id`, no SQL types, so each dialect's defaults.
pub fn book_res() -> Resource {
    Resource::new(
        book_md(),
        "books",
        "id",
        vec![
            Column::new("id", Kind::String).primary_key(),
            Column::new("title", Kind::String).not_null(),
            Column::new("published_year", Kind::Int),
            Column::new("genre", Kind::Enum).not_null(),
        ],
    )
}

/// Go's `uniqueRes`: a table of its own, and `title` unique, so the constraint is exercised.
pub fn unique_res() -> Resource {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut res = book_res();
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    res.table = format!("books_{}_{n}", std::process::id());
    for c in &mut res.columns {
        c.unique |= c.name == "title";
    }
    res
}

/// Go's `newBook`.
pub fn new_book(id: &str, title: &str, year: i32, genre: i32) -> DynamicMessage {
    let mut m = DynamicMessage::new(book_md());
    m.set_field_by_name("id", P::String(id.into()));
    m.set_field_by_name("title", P::String(title.into()));
    m.set_field_by_name("published_year", P::I32(year));
    m.set_field_by_name("genre", P::EnumNumber(genre));
    m
}

/// A string field of `m`.
pub fn text(m: &DynamicMessage, name: &str) -> String {
    m.get_field_by_name(name)
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// An int32 field of `m`.
pub fn int(m: &DynamicMessage, name: &str) -> i32 {
    m.get_field_by_name(name)
        .and_then(|v| v.as_i32())
        .unwrap_or_default()
}

/// An empty in-memory SQLite database. One connection, kept forever, because an in-memory
/// database is per connection: a second would be a different, empty database.
pub async fn sqlite() -> SqlitePool {
    SqlitePoolOptions::new()
        .max_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect("sqlite::memory:")
        .await
        .unwrap()
}

/// Transaction work from a closure, so the closure's lifetime is the higher-ranked one
/// [`TxWork`] asks for.
pub fn work<F>(f: F) -> TxWork
where
    F: for<'a> FnOnce(&'a DB) -> BoxFuture<'a, database::Result<()>> + Send + 'static,
{
    Box::new(f)
}
