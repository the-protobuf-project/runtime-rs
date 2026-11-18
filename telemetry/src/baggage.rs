//! W3C Baggage: request-scoped key/values that travel with a trace.
//!
//! An [`Observer`](crate::Observer) copies the baggage an operation runs under
//! onto its span and its completion log as `baggage.<key>`; it is never a
//! metric label, its values being unbounded. [`inject`](crate::inject) and
//! [`extract`](crate::extract) carry it in the `baggage` header, encoded as
//! runtime-go's SDK propagator encodes it.
use opentelemetry::{Context, KeyValue, baggage::BaggageExt};

/// Returns `cx` with `key=value` added to its W3C baggage, which [`inject`](crate::inject)
/// sends on with the trace. Use it for what every hop needs (a tenant, a
/// request class), never for large or secret values: it rides on every
/// outgoing message and request.
pub fn set_baggage(cx: &Context, key: impl Into<String>, value: impl Into<String>) -> Context {
    let key = key.into();
    let mut entries: Vec<KeyValue> = cx
        .baggage()
        .iter()
        .filter(|(k, _)| k.as_str() != key)
        .map(|(k, (v, _))| KeyValue::new(k.clone(), v.clone()))
        .collect();
    entries.push(KeyValue::new(key, value.into()));
    cx.with_baggage(entries)
}

/// The W3C baggage `cx` carries, sorted by key.
pub fn baggage(cx: &Context) -> Vec<(String, String)> {
    let mut entries: Vec<(String, String)> = cx
        .baggage()
        .iter()
        .map(|(k, (v, _))| (k.to_string(), v.to_string()))
        .collect();
    entries.sort();
    entries
}

/// The `baggage` header value for `cx`, or `None` when it carries none.
pub(crate) fn header(cx: &Context) -> Option<String> {
    let entries = baggage(cx);
    (!entries.is_empty()).then(|| {
        entries
            .iter()
            .map(|(k, v)| format!("{}={}", encode(k), encode(v)))
            .collect::<Vec<_>>()
            .join(",")
    })
}

/// `cx` carrying the baggage a `baggage` header lists; members without a key,
/// or with a malformed escape, are dropped.
pub(crate) fn with_header(cx: Context, header: &str) -> Context {
    let entries: Vec<KeyValue> = header
        .split(',')
        .filter_map(|member| {
            // Properties after `;` are carried by no runtime; drop them.
            let pair = member.split(';').next()?.trim();
            let (k, v) = pair.split_once('=')?;
            let (k, v) = (decode(k.trim())?, decode(v.trim())?);
            (!k.is_empty()).then(|| KeyValue::new(k, v))
        })
        .collect();
    if entries.is_empty() {
        cx
    } else {
        cx.with_baggage(entries)
    }
}

/// Percent-encodes everything W3C baggage does not allow bare in a key or value.
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"!#$&'*+-.^_`|~/:<>?@[]{}()".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Decodes [`encode`]'s escapes; `None` for a malformed one or invalid UTF-8.
fn decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = s.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}
