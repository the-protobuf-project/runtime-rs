//! Reads the `google.api` annotations the error interceptors describe resources with.
//!
//! Go asks `proto.GetExtension` for a generated extension type. Rust has no generated
//! `google.api` types here, and needs none: the annotations are extension fields on the
//! descriptors' options, which [`prost_reflect`] decodes against the same pool the
//! descriptors came from. So each annotation is read by its full name, and an API whose
//! descriptor set does not include `google/api/*.proto` simply has none.

use std::collections::HashMap;

use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, MessageDescriptor, MethodDescriptor,
};

/// What a message's `google.api.resource` annotation says about it.
#[derive(Debug, Clone, Default)]
pub(crate) struct Resource {
    /// `shop.example.com/Shelf`.
    pub(crate) kind: String,
    /// `shelves/{shelf}`, and any further patterns. Never empty.
    pub(crate) patterns: Vec<String>,
    /// `shelf`.
    pub(crate) singular: String,
    /// `shelves`.
    pub(crate) plural: String,
}

/// The message's resource annotation, when it has one with at least one pattern — the
/// only kind Go treats as a resource.
pub(crate) fn resource(message: &MessageDescriptor) -> Option<Resource> {
    let options = message.options();
    let res = extension(&options, message.parent_pool(), "google.api.resource")?;
    let patterns = strings(&res, "pattern");
    if patterns.is_empty() {
        return None;
    }
    Some(Resource {
        kind: string(&res, "type"),
        patterns,
        singular: string(&res, "singular"),
        plural: string(&res, "plural"),
    })
}

/// A field's `google.api.resource_reference`: its `type` and `child_type`, either of which
/// may be empty.
pub(crate) fn resource_reference(field: &FieldDescriptor) -> Option<(String, String)> {
    let options = field.options();
    let reference = extension(
        &options,
        field.parent_pool(),
        "google.api.resource_reference",
    )?;
    Some((string(&reference, "type"), string(&reference, "child_type")))
}

/// A method's REST binding from `google.api.http`: the verb and the path template.
///
/// The verbs are tried in the order Go tries them, which leaves out `put` and `custom`
/// exactly as it does.
pub(crate) fn http_rule(method: &MethodDescriptor) -> Option<(&'static str, String)> {
    let options = method.options();
    let rule = extension(&options, method.parent_pool(), "google.api.http")?;
    [
        ("get", "GET"),
        ("post", "POST"),
        ("patch", "PATCH"),
        ("delete", "DELETE"),
    ]
    .into_iter()
    .find_map(|(field, verb)| {
        let path = string(&rule, field);
        (!path.is_empty()).then_some((verb, path))
    })
}

/// Every resource type in the pool, mapped to its first pattern.
pub(crate) fn resource_patterns(pool: &DescriptorPool) -> HashMap<String, String> {
    pool.all_messages()
        .filter_map(|message| resource(&message))
        .map(|res| (res.kind, res.patterns[0].clone()))
        .collect()
}

/// The value of one extension on an options message, when it is set.
fn extension(
    options: &DynamicMessage,
    pool: &DescriptorPool,
    name: &str,
) -> Option<DynamicMessage> {
    let ext = pool.get_extension_by_name(name)?;
    if !options.has_extension(&ext) {
        return None;
    }
    options.get_extension(&ext).as_message().cloned()
}

fn string(message: &DynamicMessage, field: &str) -> String {
    message
        .get_field_by_name(field)
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

fn strings(message: &DynamicMessage, field: &str) -> Vec<String> {
    message
        .get_field_by_name(field)
        .and_then(|v| {
            v.as_list().map(|items| {
                items
                    .iter()
                    .filter_map(|item| item.as_str().map(str::to_string))
                    .collect()
            })
        })
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The shop API the tests describe, with its imports.
    pub(crate) fn shop() -> DescriptorPool {
        DescriptorPool::decode(&include_bytes!("../tests/fixtures/shop.binpb")[..])
            .expect("the shop descriptor set decodes")
    }

    #[test]
    fn reads_resources_references_and_http_rules() {
        let pool = shop();
        let shelf = resource(&pool.get_message_by_name("shop.v1.Shelf").unwrap()).unwrap();
        assert_eq!(shelf.kind, "shop.example.com/Shelf");
        assert_eq!(shelf.patterns, ["shelves/{shelf}"]);
        assert_eq!(
            (shelf.singular.as_str(), shelf.plural.as_str()),
            ("shelf", "shelves")
        );
        assert!(resource(&pool.get_message_by_name("shop.v1.GetShelfRequest").unwrap()).is_none());

        let parent = pool
            .get_message_by_name("shop.v1.ListBooksRequest")
            .unwrap()
            .get_field_by_name("parent")
            .unwrap();
        assert_eq!(
            resource_reference(&parent),
            Some((String::new(), "shop.example.com/Book".to_string()))
        );

        let service = pool.get_service_by_name("shop.v1.ShopService").unwrap();
        let undelete = service
            .methods()
            .find(|m| m.name() == "UndeleteShelf")
            .unwrap();
        assert_eq!(
            http_rule(&undelete),
            Some(("POST", "/v1/{name=shelves/*}:undelete".to_string()))
        );
        assert_eq!(
            resource_patterns(&pool)
                .get("shop.example.com/Book")
                .map(String::as_str),
            Some("shelves/{shelf}/books/{book}")
        );
    }
}
