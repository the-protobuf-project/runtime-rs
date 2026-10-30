//! Extra properties: fields added to every tool's schema and lifted back out of the
//! arguments before the request is decoded.
//!
//! They exist for values a caller must supply but the proto has no field for — an API key,
//! a tenant id — which would otherwise have to be smuggled through a real request field.

use serde_json::{Map, Value};

/// One property injected into tool schemas and extracted from incoming arguments.
///
/// Go carries a `ContextKey any` here and stashes the value on the request context. A hosted
/// service does the same with [`ExtraValues`], a request extension keyed by
/// [`ExtraProperty::context_key`]; [`extract_extras`] is the same split for a caller that
/// wants the values in hand.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtraProperty {
    /// The JSON property name, as it appears in tool arguments.
    pub name: String,
    /// Shown in the tool schema.
    pub description: String,
    /// Whether the schema marks it required.
    pub required: bool,
    /// The key the value is stored under in [`ExtraValues`]. `None` means the name.
    pub context_key: Option<String>,
}

/// The extras one tool call carried, keyed by [`ExtraProperty::context_key`].
///
/// A hosted service lifts them out of the arguments before the handler sees them and puts
/// them on the call's `RequestContext::extensions`, which is where Go's `ContextKey` puts them
/// on its context.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExtraValues(pub Map<String, Value>);

impl ExtraProperty {
    /// An optional extra property.
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            required: false,
            context_key: None,
        }
    }

    /// Stores the value under `key` in [`ExtraValues`] rather than under the property name.
    #[must_use]
    pub fn with_context_key(mut self, key: impl Into<String>) -> Self {
        self.context_key = Some(key.into());
        self
    }

    /// The key the value is stored under.
    pub fn key(&self) -> &str {
        self.context_key.as_deref().unwrap_or(&self.name)
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
