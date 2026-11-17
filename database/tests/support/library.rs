//! A two-resource fixture shaped as protoc-gen-store generates a Go-compatible table: a
//! root `Writer` and a `Novel` nested under a publisher, each keyed by a field-less ULID
//! `id` and found by its unique `name`, the novel with a field-less `publisher_id` parent
//! column, an `author` reference to a writer, and a `format` enum stored by label.

#![allow(dead_code)]

use std::sync::Arc;

use database::records::{Column, Generated, Kind, Resource};
use database::repository::{Reference, Rules};
use prost_reflect::{MessageDescriptor, ReflectMessage};

#[path = "library_pool.rs"]
mod descriptors;
use descriptors::pool;

/// `lib.v1.Writer`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Writer {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub display_name: String,
}

/// `lib.v1.Novel`, under `publishers/{publisher}`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Novel {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub title: String,
    #[prost(string, tag = "3")]
    pub author: String,
    #[prost(int32, tag = "4")]
    pub format: i32,
    #[prost(string, tag = "5")]
    pub etag: String,
}

impl ReflectMessage for Writer {
    fn descriptor(&self) -> MessageDescriptor {
        pool().get_message_by_name("lib.v1.Writer").unwrap()
    }
}

impl ReflectMessage for Novel {
    fn descriptor(&self) -> MessageDescriptor {
        pool().get_message_by_name("lib.v1.Novel").unwrap()
    }
}

fn text(name: &str) -> Column {
    Column::new(name, Kind::String).sql_type("VARCHAR(64)")
}

/// The key every table has: a field-less ULID, the SQL primary key.
fn id() -> Column {
    Column {
        field: String::new(),
        generated: Some(Generated::Ulid),
        ..text("id").primary_key()
    }
}

/// `writers`, found by `name`.
pub fn writers() -> Arc<Resource> {
    let columns = vec![id(), text("name").not_null().unique(), text("display_name")];
    Arc::new(Resource::new(
        Writer::default().descriptor(),
        "writers",
        "name",
        columns,
    ))
}

/// `novels`, found by `name`; `format_type` is the Postgres enum type `format` is stored
/// as, empty for a dialect storing text.
pub fn novels(format_type: &str) -> Arc<Resource> {
    let format = Column {
        labels: vec![
            (1, "HARDCOVER".into()),
            (2, "PAPERBACK".into()),
            (3, "EBOOK".into()),
        ],
        ..Column::new("format", Kind::String).sql_type(format_type)
    };
    let parent = Column {
        field: String::new(),
        ..text("publisher_id").not_null()
    };
    let columns = vec![
        id(),
        text("name").not_null().unique(),
        text("title"),
        text("author"),
        format,
        text("etag"),
        parent,
    ];
    Arc::new(Resource::new(
        Novel::default().descriptor(),
        "novels",
        "name",
        columns,
    ))
}

/// A writer's rules: its pattern alone.
pub fn writer_rules() -> Rules {
    Rules {
        pattern: "writers/{writer}".into(),
        ..Rules::default()
    }
}

/// A novel's rules: pattern, parent column, the author reference and the etag.
pub fn novel_rules() -> Rules {
    Rules {
        etag: Some("etag".into()),
        pattern: "publishers/{publisher}/novels/{novel}".into(),
        parent_column: Some("publisher_id".into()),
        references: vec![Reference {
            field: "author".into(),
            pattern: "writers/{writer}".into(),
        }],
        ..Rules::default()
    }
}

/// A novel named `publishers/<publisher>/novels/<id>`.
pub fn novel(publisher: &str, id: &str, title: &str) -> Novel {
    Novel {
        name: format!("publishers/{publisher}/novels/{id}"),
        title: title.into(),
        ..Default::default()
    }
}
