//! An `Article` resource for the repository tests: the shared `Book` fixture's shape plus
//! what a repository manages (an etag, AIP-164 `delete_time` and `expire_time`, an
//! output-only counter), a message field and a oneof for field masks, and a prost type so a
//! `RecordsRepository<Article>` is typed as a generated one is.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use database::records::{Column, Kind, Resource};
use database::repository::Rules;
use prost_reflect::{DescriptorPool, MessageDescriptor, ReflectMessage};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, OneofDescriptorProto, Timestamp,
};

/// `test.v1.Article` as prost would generate it.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Article {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub title: String,
    #[prost(int32, tag = "3")]
    pub pages: i32,
    #[prost(string, optional, tag = "4")]
    pub summary: Option<String>,
    #[prost(int64, tag = "5")]
    pub views: i64,
    #[prost(message, optional, tag = "6")]
    pub publish_time: Option<Timestamp>,
    #[prost(message, optional, tag = "7")]
    pub create_time: Option<Timestamp>,
    #[prost(message, optional, tag = "8")]
    pub update_time: Option<Timestamp>,
    #[prost(message, optional, tag = "9")]
    pub delete_time: Option<Timestamp>,
    #[prost(message, optional, tag = "10")]
    pub expire_time: Option<Timestamp>,
    #[prost(string, tag = "11")]
    pub etag: String,
    #[prost(oneof = "Format", tags = "12, 13")]
    pub format: Option<Format>,
}

/// The `format` oneof: one identifier or the other.
#[derive(Clone, PartialEq, prost::Oneof)]
pub enum Format {
    #[prost(string, tag = "12")]
    Isbn(String),
    #[prost(string, tag = "13")]
    Doi(String),
}

impl ReflectMessage for Article {
    fn descriptor(&self) -> MessageDescriptor {
        article_descriptor()
    }
}

/// The descriptor, built once without protoc.
pub fn article_descriptor() -> MessageDescriptor {
    static ARTICLE: std::sync::OnceLock<MessageDescriptor> = std::sync::OnceLock::new();
    ARTICLE.get_or_init(build).clone()
}

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

fn build() -> MessageDescriptor {
    let mut pool = DescriptorPool::new();
    let ts = DescriptorPool::global()
        .get_file_by_name("google/protobuf/timestamp.proto")
        .expect("prost-reflect carries the well-known types");
    pool.add_file_descriptor_proto(ts.file_descriptor_proto().clone())
        .unwrap();
    let time = |name: &str, number| FieldDescriptorProto {
        type_name: Some(".google.protobuf.Timestamp".into()),
        ..field(name, number, Type::Message)
    };
    let arm = |name: &str, number| FieldDescriptorProto {
        oneof_index: Some(0),
        ..field(name, number, Type::String)
    };
    let oneof = |name: &str| OneofDescriptorProto {
        name: Some(name.into()),
        ..Default::default()
    };
    let article = DescriptorProto {
        name: Some("Article".into()),
        field: vec![
            field("name", 1, Type::String),
            field("title", 2, Type::String),
            field("pages", 3, Type::Int32),
            FieldDescriptorProto {
                oneof_index: Some(1),
                proto3_optional: Some(true),
                ..field("summary", 4, Type::String)
            },
            field("views", 5, Type::Int64),
            time("publish_time", 6),
            time("create_time", 7),
            time("update_time", 8),
            time("delete_time", 9),
            time("expire_time", 10),
            field("etag", 11, Type::String),
            arm("isbn", 12),
            arm("doi", 13),
        ],
        oneof_decl: vec![oneof("format"), oneof("_summary")],
        ..Default::default()
    };
    pool.add_file_descriptor_proto(FileDescriptorProto {
        name: Some("test/v1/article.proto".into()),
        package: Some("test.v1".into()),
        dependency: vec!["google/protobuf/timestamp.proto".into()],
        message_type: vec![article],
        syntax: Some("proto3".into()),
        ..Default::default()
    })
    .unwrap();
    pool.get_message_by_name("test.v1.Article").unwrap()
}

/// The `Article` resource in table `table`, keyed by `name`, `title` unique; every type left
/// to the dialect's default but the strings, which MySQL needs bounded to index.
pub fn resource(table: &str) -> Arc<Resource> {
    let text = |n: &str| Column::new(n, Kind::String).sql_type("VARCHAR(255)");
    let mut create = Column::new("create_time", Kind::Timestamp);
    create.auto_create = true;
    let mut update = Column::new("update_time", Kind::Timestamp);
    update.auto_update = true;
    Arc::new(Resource::new(
        article_descriptor(),
        table,
        "name",
        vec![
            text("name").primary_key(),
            text("title").not_null().unique(),
            Column::new("pages", Kind::Int).not_null(),
            text("summary"),
            Column::new("views", Kind::Int).not_null(),
            Column::new("publish_time", Kind::Timestamp),
            create,
            update,
            Column::new("delete_time", Kind::Timestamp),
            Column::new("expire_time", Kind::Timestamp),
            text("etag").not_null(),
            text("isbn"),
            text("doi"),
        ],
    ))
}

/// The rules protoc-gen-store derives from the field behaviors: soft delete with
/// `retention`, and the output-only fields.
pub fn rules(retention: Duration) -> Rules {
    Rules {
        etag: Some("etag".into()),
        delete_time: Some("delete_time".into()),
        expire_time: Some("expire_time".into()),
        retention,
        output_only: [
            "views",
            "create_time",
            "update_time",
            "delete_time",
            "expire_time",
        ]
        .map(String::from)
        .to_vec(),
    }
}

/// The rules of a resource without soft delete: deletes remove the record.
pub fn hard_rules() -> Rules {
    Rules {
        delete_time: None,
        expire_time: None,
        ..rules(Duration::ZERO)
    }
}

/// An article named `articles/<id>`, titled `title`, of `pages` pages.
pub fn article(id: &str, title: &str, pages: i32) -> Article {
    Article {
        name: format!("articles/{id}"),
        title: title.into(),
        pages,
        ..Default::default()
    }
}
