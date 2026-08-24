//! Extra properties: fields added to every tool's schema and lifted back out of the
//! arguments before the request is decoded.
//!
//! They exist for values a caller must supply but the proto has no field for — an API key,
//! a tenant id — which would otherwise have to be smuggled through a real request field.

use serde_json::{Map, Value};

/// One property injected into tool schemas and extracted from incoming arguments.
///
/// Go carries a `ContextKey any` here and stashes the value on the request context. Rust has
/// no untyped context to stash into, so [`extract_extras`] returns the values instead and the
/// caller decides where they go — which is also what makes them visible rather than ambient.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtraProperty {
    /// The JSON property name, as it appears in tool arguments.
    pub name: String,
    /// Shown in the tool schema.
    pub description: String,
    /// Whether the schema marks it required.
    pub required: bool,
}

impl ExtraProperty {
    /// An optional extra property.
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            required: false,
        }
    }

    /// Marks it required, so a client that omits it is rejected by schema validation rather
    /// than by the handler.
    #[must_use]
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }
}

/// Splits incoming arguments into the extras that were declared and the rest.
///
/// The remaining arguments are what gets decoded into the request message, so an extra never
/// reaches a proto field it does not belong to. Arguments that are not an object are passed
/// through untouched, since there is nothing to split.
pub fn extract_extras(arguments: Value, extras: &[ExtraProperty]) -> (Value, Map<String, Value>) {
    let mut found = Map::new();
    if extras.is_empty() {
        return (arguments, found);
    }

    let Value::Object(mut map) = arguments else {
        return (arguments, found);
    };

    for extra in extras {
        if let Some(value) = map.remove(&extra.name) {
            found.insert(extra.name.clone(), value);
        }
    }

    (Value::Object(map), found)
}
