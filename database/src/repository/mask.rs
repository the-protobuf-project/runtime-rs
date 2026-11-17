//! AIP-134 field masks over a stored record: which fields a mask writes, and the merge. Go's
//! generated `apply<Model>Mask` with `repox.InMask` and `repox.GroupTouched`.

use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor};

use crate::{Error, Result};

/// The fields `paths` writes, out of `allowed`. An empty mask, or AIP-134's `*` alone, is
/// every allowed field. A path into a message field (`publish_time.seconds`) writes the whole
/// field, and a path to one arm of a oneof the whole oneof, as Go replaces both wholesale. A
/// path naming a field that exists but is not allowed (output-only, the etag, an audit time)
/// is ignored, as AIP-203 asks; one naming no field is refused.
pub(super) fn plan(
    msg: &MessageDescriptor,
    paths: &[String],
    allowed: &[FieldDescriptor],
) -> Result<Vec<FieldDescriptor>> {
    if paths.is_empty() || paths == ["*"] {
        return Ok(allowed.to_vec());
    }
    let mut out: Vec<FieldDescriptor> = Vec::new();
    for path in paths {
        let top = resolve(msg, path)?;
        let group: Vec<FieldDescriptor> = match top.containing_oneof() {
            Some(oneof) if !oneof.is_synthetic() => oneof.fields().collect(),
            _ => vec![top],
        };
        for f in group {
            if allowed.contains(&f) && !out.contains(&f) {
                out.push(f);
            }
        }
    }
    Ok(out)
}

/// Copies each of `fields` from `src` onto `dst`, clearing it where `src` has none, as Go's
/// `merged.X = in.GetX()` leaves the zero value.
pub(super) fn copy(dst: &mut DynamicMessage, src: &DynamicMessage, fields: &[FieldDescriptor]) {
    // Clears first, then sets: clearing one arm of a oneof after setting another would not
    // undo it, but setting one arm clears its siblings, so this order is the oneof's merge.
    for f in fields.iter().filter(|f| !src.has_field(f)) {
        dst.clear_field(f);
    }
    for f in fields.iter().filter(|f| src.has_field(f)) {
        dst.set_field(f, src.get_field(f).into_owned());
    }
}

/// The top-level field `path` starts at, once every segment is proved to name a field: each
/// one before the last must be a singular message holding the next.
fn resolve(msg: &MessageDescriptor, path: &str) -> Result<FieldDescriptor> {
    let bad = |why: String| Error::Invalid(format!("field mask path {path:?}: {why}"));
    let mut segments = path.split('.');
    let mut desc = msg.clone();
    let mut top = None;
    let mut parent: Option<FieldDescriptor> = None;
    for seg in segments.by_ref() {
        if let Some(p) = &parent {
            desc = match p.kind() {
                Kind::Message(m) if !p.is_list() && !p.is_map() => m,
                _ => return Err(bad(format!("{} holds no fields", p.full_name()))),
            };
        }
        let f = desc
            .get_field_by_name(seg)
            .ok_or_else(|| bad(format!("{} has no field {seg:?}", desc.full_name())))?;
        top.get_or_insert_with(|| f.clone());
        parent = Some(f);
    }
    top.ok_or_else(|| bad("empty".into()))
}
