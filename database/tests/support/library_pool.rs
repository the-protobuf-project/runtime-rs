//! The descriptors of the library fixture, `lib.v1.Writer`, `lib.v1.Novel` and its
//! `Format` enum, built without protoc.

use prost_reflect::DescriptorPool;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto,
};

fn field(name: &str, number: i32, ty: Type) -> FieldDescriptorProto {
    let (name, number, ty) = (Some(name.into()), Some(number), Some(ty as i32));
    let label = Some(Label::Optional as i32);
    FieldDescriptorProto {
        json_name: name.clone(),
        name,
        number,
        label,
        r#type: ty,
        ..Default::default()
    }
}

pub(super) fn pool() -> &'static DescriptorPool {
    static POOL: std::sync::OnceLock<DescriptorPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let message = |name: &str, field: Vec<FieldDescriptorProto>| DescriptorProto {
            name: Some(name.into()),
            field,
            ..Default::default()
        };
        let values = [
            "FORMAT_UNSPECIFIED",
            "FORMAT_HARDCOVER",
            "FORMAT_PAPERBACK",
            "FORMAT_EBOOK",
        ];
        let format = EnumDescriptorProto {
            name: Some("Format".into()),
            value: (0..)
                .zip(values)
                .map(|(i, n)| EnumValueDescriptorProto {
                    name: Some(n.into()),
                    number: Some(i),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(FileDescriptorProto {
            name: Some("lib/v1/lib.proto".into()),
            package: Some("lib.v1".into()),
            message_type: vec![
                message(
                    "Writer",
                    vec![
                        field("name", 1, Type::String),
                        field("display_name", 2, Type::String),
                    ],
                ),
                message(
                    "Novel",
                    vec![
                        field("name", 1, Type::String),
                        field("title", 2, Type::String),
                        field("author", 3, Type::String),
                        FieldDescriptorProto {
                            type_name: Some(".lib.v1.Format".into()),
                            ..field("format", 4, Type::Enum)
                        },
                        field("etag", 5, Type::String),
                    ],
                ),
            ],
            enum_type: vec![format],
            syntax: Some("proto3".into()),
            ..Default::default()
        })
        .unwrap();
        pool
    })
}
