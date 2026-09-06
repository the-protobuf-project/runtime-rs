//! Asking the user for input in the middle of a tool call.
//!
//! # How this differs from the Go runtime
//!
//! Go implements the SEP-2322 handshake by hand: a handler returns a result carrying an
//! `InputRequests` map, the client answers, and the handler runs a second time. rmcp models
//! elicitation as an ordinary server-to-client request instead — [`elicit`] awaits the answer
//! inline — so there is no pending result to return and no second invocation to guard
//! against. `ElicitRequestId` and `RunElicitation`'s two-phase shape have no counterpart here
//! because the round trip they exist to manage is handled by the SDK.

use rmcp::model::{
    ElicitRequestParams, ElicitationAction, ElicitationSchema, PrimitiveSchemaDefinition,
};
use rmcp::service::{Peer, RoleServer};
use serde_json::{Map, Value};

/// One field on an elicitation form.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ElicitField {
    /// The JSON property name.
    pub name: String,
    /// Shown in the form.
    pub description: String,
    /// Whether the user must provide a value.
    pub required: bool,
    /// The JSON Schema type: `string`, `number`, `integer`, or `boolean`.
    pub field_type: String,
    /// The friendly names shown in the form, when the field is a closed set.
    pub enum_values: Vec<String>,
    /// The protobuf enum names, parallel to [`ElicitField::enum_values`].
    ///
    /// They are carried separately so a form can show `High priority` while the request that
    /// follows still decodes as `PRIORITY_HIGH` — see [`merge_elicit_result`].
    pub proto_values: Vec<String>,
}

impl ElicitField {
    /// A field of the given JSON Schema type.
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        field_type: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            field_type: field_type.into(),
            ..Default::default()
        }
    }

    /// Marks the field required.
    #[must_use]
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Restricts the field to a closed set, optionally mapping each shown name to the proto
    /// enum name it stands for.
    #[must_use]
    pub fn with_enum(mut self, shown: Vec<String>, proto: Vec<String>) -> Self {
        self.enum_values = shown;
        self.proto_values = proto;
        self
    }
}

/// Builds the schema an elicitation form is rendered from.
pub fn elicit_schema(fields: &[ElicitField]) -> ElicitationSchema {
    let mut properties: Vec<(String, PrimitiveSchemaDefinition)> = Vec::new();
    let mut required: Vec<String> = Vec::new();

    for field in fields {
        let mut definition = Map::new();
        definition.insert("type".into(), Value::String(field.field_type.clone()));
        definition.insert(
            "description".into(),
            Value::String(field.description.clone()),
        );
        if !field.enum_values.is_empty() {
            definition.insert(
                "enum".into(),
                Value::Array(
                    field
                        .enum_values
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            );
        }

        // A field the SDK cannot render as a primitive is dropped rather than failing the
        // whole form: the rest is still answerable, and a rejected form asks nothing at all.
        if let Ok(primitive) =
            serde_json::from_value::<PrimitiveSchemaDefinition>(Value::Object(definition))
        {
            properties.push((field.name.clone(), primitive));
            if field.required {
                required.push(field.name.clone());
            }
        }
    }

    // ElicitationSchema is #[non_exhaustive], so it is assembled through its builder.
    let mut builder = ElicitationSchema::builder();
    for (name, definition) in properties {
        builder = if required.contains(&name) {
            builder.required_property(name, definition)
        } else {
            builder.property(name, definition)
        };
    }
    // build() fails only when a required name is absent from properties, and
    // required_property inserts into both — so the invariant holds by construction.
    builder
        .build()
        .expect("every required field was added as a property")
}

/// Asks the client to fill in `fields` and waits for the answer.
///
/// `Ok(None)` means the user declined or cancelled, which is not an error: a tool that asks
/// for confirmation and is told no has its answer.
pub async fn elicit(
    peer: &Peer<RoleServer>,
    message: impl Into<String>,
    fields: &[ElicitField],
) -> Result<Option<Value>, rmcp::ServiceError> {
    let response = peer
        .create_elicitation(ElicitRequestParams::FormElicitationParams {
            meta: None,
            message: message.into(),
            requested_schema: elicit_schema(fields),
        })
        .await?;

    match response.action {
        ElicitationAction::Accept => Ok(response.content),
        _ => Ok(None),
    }
}

/// Overlays an accepted elicitation answer onto the tool's original arguments.
///
/// Fields carrying [`ElicitField::proto_values`] are mapped back from the name the form
/// showed to the protobuf enum name, so the merged arguments decode into the request message
/// rather than failing on a value the proto has never heard of.
pub fn merge_elicit_result(arguments: Value, content: &Value, fields: &[ElicitField]) -> Value {
    let Some(answers) = content.as_object() else {
        return arguments;
    };
    if answers.is_empty() {
        return arguments;
    }

    let mut merged = match arguments {
        Value::Object(map) => map,
        _ => Map::new(),
    };

    for (key, value) in answers {
        let mapped = fields
            .iter()
            .find(|f| &f.name == key)
            .filter(|f| f.proto_values.len() == f.enum_values.len())
            .and_then(|f| {
                let shown = value.as_str()?;
                let index = f.enum_values.iter().position(|v| v == shown)?;
                Some(Value::String(f.proto_values[index].clone()))
            });
        merged.insert(key.clone(), mapped.unwrap_or_else(|| value.clone()));
    }

    Value::Object(merged)
}
