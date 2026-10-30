//! Turns a `buf.validate` failure into an error a developer can act on.
//!
//! protovalidate reports what rule failed, in the rule's terms: a field "does not match
//! regex pattern `^organizationalUnits/[^/]+$`". That is accurate and unhelpful — the
//! caller has to reverse-engineer a regex into the value it wanted. For the fields where
//! that matters most, resource names, the proto already says what the value looks like: a
//! `google.api.resource` pattern. So a violation is described by what the field must be,
//! with the pattern as the example and the value that was sent beside it, and each one also
//! travels as a `google.rpc.BadRequest` field violation a client can show next to the input
//! it belongs to.
//!
//! This is Go's `validation_hints.go` over [`prost_protovalidate`]'s violations. The one
//! difference is where resource patterns come from: Go walks its global registry, while
//! here they are read from the pool the violated field's descriptor belongs to — the
//! descriptor sets registered with [`crate::HybridServer::with_reflection`].

use prost_protovalidate::Violation;
use prost_reflect::{FieldDescriptor, Kind, Value};
use tonic::{Code, Status};
use tonic_types::{BadRequest, ErrorDetail, ErrorInfo, FieldViolation, StatusExt};

/// The `ErrorInfo` reason of a request that failed its `buf.validate` rules.
pub const VALIDATION_REASON: &str = "INVALID_REQUEST_FIELDS";

/// The `INVALID_ARGUMENT` status for a protovalidate error: one line per violated field in
/// the message, and a `BadRequest` detail with the same, field by field, beside an
/// `ErrorInfo` whose reason is [`VALIDATION_REASON`].
///
/// It is public so a custom [`crate::Validator`] built on the same engine reports failures
/// exactly as the default one does. An error that is not a set of violations — a rule that
/// failed to compile — is still `INVALID_ARGUMENT`, carrying its text, as in Go.
pub fn validation_status(err: &prost_protovalidate::Error) -> Status {
    let violations = match err {
        prost_protovalidate::Error::Validation(verr) if !verr.is_empty() => verr.violations(),
        other => return Status::invalid_argument(other.to_string()),
    };

    let mut lines = Vec::with_capacity(violations.len());
    let mut fields = Vec::with_capacity(violations.len());
    for violation in violations {
        let field = violation.field_path();
        let line = join_field(&field, &describe(violation));
        fields.push(FieldViolation {
            reason: violation.rule_id().to_string(),
            ..FieldViolation::new(field, line.clone())
        });
        lines.push(line);
    }

    let mut message = lines[0].clone();
    if !message.ends_with('.') {
        message.push('.');
    }
    if lines.len() > 1 {
        message = format!(
            "{} fields need fixing:\n - {}",
            lines.len(),
            lines.join("\n - ")
        );
    }
    tracing::debug!(violations = lines.len(), "validation: request rejected");
    Status::with_error_details_vec(
        Code::InvalidArgument,
        message,
        vec![
            ErrorDetail::BadRequest(BadRequest::new(fields)),
            ErrorDetail::ErrorInfo(ErrorInfo::new(
                VALIDATION_REASON,
                "",
                std::collections::HashMap::new(),
            )),
        ],
    )
}

/// Reads a description after its field: "window is required", "parent must look like
/// ...", and otherwise "page_size: <rule message>".
fn join_field(field: &str, desc: &str) -> String {
    if desc.starts_with("is ") || desc.starts_with("must ") {
        format!("{field} {desc}")
    } else {
        format!("{field}: {desc}")
    }
}

/// Says what the field must be.
fn describe(violation: &Violation) -> String {
    let field = violation.field_descriptor();
    let rule = violation.rule_id();
    if rule == "required" {
        if let Some(pattern) = field.and_then(name_pattern) {
            return format!("is required: the name of a resource, {pattern}");
        }
        return "is required".to_string();
    }
    if rule.ends_with(".pattern")
        && let Some(fd) = field.filter(|fd| fd.kind() == Kind::String && !fd.is_list())
    {
        let pattern = name_pattern(fd).or_else(|| {
            violation
                .rule_value()
                .and_then(Value::as_str)
                .and_then(pattern_template)
        });
        if let Some(pattern) = pattern {
            return format!("must look like {pattern}; got {}", sent(violation));
        }
    }

    let message = violation.message().to_string();
    let scalar = field.is_some_and(|fd| !fd.is_list() && !fd.is_map() && !is_message(fd));
    match sent(violation) {
        got if scalar && !got.is_empty() => format!("{message}; got {got}"),
        _ => message,
    }
}

/// Renders the value a violation is about, for the message; empty for a value that does
/// not read on one line — a message, a list, a map.
fn sent(violation: &Violation) -> String {
    let (Some(fd), Some(value)) = (violation.field_descriptor(), violation.field_value()) else {
        return String::new();
    };
    if fd.is_list() || fd.is_map() || is_message(fd) {
        return String::new();
    }
    match value {
        Value::String(s) if s.is_empty() => "nothing".to_string(),
        Value::String(s) => format!("{s:?}"),
        Value::Bool(v) => v.to_string(),
        Value::I32(v) => v.to_string(),
        Value::I64(v) => v.to_string(),
        Value::U32(v) => v.to_string(),
        Value::U64(v) => v.to_string(),
        Value::F32(v) => v.to_string(),
        Value::F64(v) => v.to_string(),
        Value::EnumNumber(v) => v.to_string(),
        Value::Bytes(v) => format!("{} bytes", v.len()),
        Value::Message(_) | Value::List(_) | Value::Map(_) => String::new(),
    }
}

fn is_message(fd: &FieldDescriptor) -> bool {
    matches!(fd.kind(), Kind::Message(_))
}

/// The resource-name pattern a string field holds, when the proto says: a
/// `resource_reference` names the type (or, for a parent, the child type, whose pattern
/// minus its last collection is the parent's), and an AIP-122 `name` field is its own
/// message's resource.
fn name_pattern(fd: &FieldDescriptor) -> Option<String> {
    if fd.kind() != Kind::String {
        return None;
    }
    if let Some((kind, child)) = crate::annotations::resource_reference(fd) {
        let patterns = crate::annotations::resource_patterns(fd.parent_pool());
        if let Some(pattern) = patterns.get(&kind) {
            return Some(pattern.clone());
        }
        if let Some(child) = patterns.get(&child) {
            let segments: Vec<&str> = child.split('/').collect();
            if segments.len() >= 4 {
                return Some(segments[..segments.len() - 2].join("/"));
            }
        }
    }
    if fd.name() == "name" {
        return crate::annotations::resource(fd.parent_message())
            .map(|res| res.patterns[0].clone());
    }
    None
}

/// Turns the common resource-name regex into the template it encodes —
/// `^organizationalUnits/[^/]+$` into `organizationalUnits/{id}` — and `None` for any other
/// regex, which is then reported as written.
fn pattern_template(regex: &str) -> Option<String> {
    let template = regex.strip_prefix('^').unwrap_or(regex);
    let template = template.strip_suffix('$').unwrap_or(template);
    let template = template.replace("[^/]+", "{id}");
    simple_template(&template).then_some(template)
}

/// Go's `^[A-Za-z0-9_\-]+(/(\{id\}|[A-Za-z0-9_\-]+))*$`: a literal first segment, then
/// literals or `{id}`.
fn simple_template(template: &str) -> bool {
    let literal = |s: &str| {
        !s.is_empty()
            && s.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    };
    let mut segments = template.split('/');
    segments.next().is_some_and(literal) && segments.all(|s| s == "{id}" || literal(s))
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_reflect::DynamicMessage;

    #[test]
    fn regexes_read_as_templates() {
        for (regex, want) in [
            (
                "^organizationalUnits/[^/]+$",
                Some("organizationalUnits/{id}"),
            ),
            (
                "^calendars/[^/]+/events/[^/]+$",
                Some("calendars/{id}/events/{id}"),
            ),
            ("^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$", None),
            (r"^-?[0-9]+(\.[0-9]+)?s$", None),
        ] {
            assert_eq!(pattern_template(regex).as_deref(), want, "{regex}");
        }
    }

    #[test]
    fn descriptions_read_after_their_field() {
        assert_eq!(join_field("window", "is required"), "window is required");
        assert_eq!(
            join_field(
                "parent",
                "must look like organizationalUnits/{id}; got \"x\""
            ),
            "parent must look like organizationalUnits/{id}; got \"x\""
        );
        assert_eq!(
            join_field("page_size", "value must be greater than or equal to 0"),
            "page_size: value must be greater than or equal to 0"
        );
    }

    fn validate(name: &str, fields: &[(&str, Value)]) -> Status {
        let pool = crate::annotations::tests::shop();
        let mut message = DynamicMessage::new(pool.get_message_by_name(name).unwrap());
        for (field, value) in fields {
            message.set_field_by_name(field, value.clone());
        }
        let err = prost_protovalidate::Validator::new()
            .validate(&message)
            .expect_err("the request breaks its rules");
        validation_status(&err)
    }

    #[test]
    fn a_resource_name_is_described_by_its_pattern() {
        let status = validate(
            "shop.v1.ListBooksRequest",
            &[
                ("parent", Value::String("books".into())),
                ("label", Value::String("labels/new".into())),
            ],
        );
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "parent must look like shelves/{shelf}; got \"books\"."
        );
        let bad = status
            .get_details_bad_request()
            .expect("a BadRequest detail");
        assert_eq!(bad.field_violations[0].field, "parent");
        assert_eq!(bad.field_violations[0].reason, "string.pattern");
        assert_eq!(
            status.get_details_error_info().map(|info| info.reason),
            Some(VALIDATION_REASON.to_string())
        );
    }

    #[test]
    fn several_violations_are_listed_one_per_line() {
        let status = validate(
            "shop.v1.ListBooksRequest",
            &[
                ("parent", Value::String("shelves/s".into())),
                ("page_size", Value::I32(-1)),
                ("label", Value::String("x".into())),
            ],
        );
        assert_eq!(
            status.message(),
            "2 fields need fixing:\n - page_size: value must be greater than or equal to 0; \
             got -1\n - label must look like labels/{id}; got \"x\""
        );
        assert_eq!(
            status
                .get_details_bad_request()
                .unwrap()
                .field_violations
                .len(),
            2
        );
    }

    #[test]
    fn a_required_name_says_what_a_name_looks_like() {
        let status = validate("shop.v1.GetBookRequest", &[]);
        assert_eq!(
            status.message(),
            "name is required: the name of a resource, shelves/{shelf}/books/{book}."
        );
    }
}
