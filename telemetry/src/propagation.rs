//! W3C Trace Context in message and request headers, without global propagators.
//!
//! `inject` writes the span a context carries as `traceparent` (and
//! `tracestate`) into any header carrier; `extract` reads them back into a
//! context whose remote span parents the receiver's own. The encoding is the
//! one runtime-go's SDK propagator writes, so a trace crosses between the two
//! languages through a stream message, an HTTP request or a gRPC call.
use opentelemetry::{
    Context,
    propagation::{Extractor, Injector},
    trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState},
};
use std::str::FromStr;

/// The W3C header carrying the trace and parent span ids.
pub const TRACEPARENT: &str = "traceparent";
/// The W3C header carrying vendor trace state.
pub const TRACESTATE: &str = "tracestate";

/// Writes the span `cx` carries into `headers`; writes nothing when it has none.
pub fn inject(cx: &Context, headers: &mut dyn Injector) {
    let span = cx.span();
    let sc = span.span_context();
    if !sc.is_valid() {
        return;
    }
    headers.set(
        TRACEPARENT,
        format!(
            "00-{}-{}-{:02x}",
            sc.trace_id(),
            sc.span_id(),
            sc.trace_flags().to_u8()
        ),
    );
    let state = sc.trace_state().header();
    if !state.is_empty() {
        headers.set(TRACESTATE, state);
    }
}

/// A context whose remote span is the one `headers` carries, for the receiver's
/// span to be its child; an empty context when they carry none or a malformed one.
pub fn extract(headers: &dyn Extractor) -> Context {
    extract_into(Context::new(), headers)
}

/// [`extract`] onto an existing context, keeping its other values.
pub fn extract_into(cx: Context, headers: &dyn Extractor) -> Context {
    match headers.get(TRACEPARENT).and_then(parse) {
        Some((trace_id, span_id, flags)) => {
            let state = headers
                .get(TRACESTATE)
                .and_then(|s| TraceState::from_str(s).ok())
                .unwrap_or_default();
            let sc = SpanContext::new(trace_id, span_id, flags, true, state);
            cx.with_remote_span_context(sc)
        }
        None => cx,
    }
}

/// The trace id of the span `cx` carries, as 32 hex digits, to quote in an error
/// so an operator can find the request's trace.
pub fn trace_id(cx: &Context) -> Option<String> {
    let span = cx.span();
    let sc = span.span_context();
    sc.is_valid().then(|| sc.trace_id().to_string())
}

/// Parses `version-traceid-spanid-flags`, refusing the all-zero ids the
/// specification calls invalid.
fn parse(header: &str) -> Option<(TraceId, SpanId, TraceFlags)> {
    let mut parts = header.trim().split('-');
    let version = parts.next()?;
    let trace = parts.next()?;
    let span = parts.next()?;
    let flags = parts.next()?;
    if version.len() != 2 || version == "ff" || trace.len() != 32 || span.len() != 16 {
        return None;
    }
    if version == "00" && parts.next().is_some() {
        return None;
    }
    let trace_id = TraceId::from_hex(trace).ok()?;
    let span_id = SpanId::from_hex(span).ok()?;
    let flags = u8::from_str_radix(flags.get(..2)?, 16).ok()?;
    if trace_id == TraceId::INVALID || span_id == SpanId::INVALID {
        return None;
    }
    Some((trace_id, span_id, TraceFlags::new(flags) & TraceFlags::SAMPLED))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const HEADER: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn a_traceparent_round_trips() {
        let headers = HashMap::from([(TRACEPARENT.to_string(), HEADER.to_string())]);
        let cx = extract(&headers);
        assert_eq!(
            trace_id(&cx).as_deref(),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
        let mut out = HashMap::new();
        inject(&cx, &mut out);
        assert_eq!(out.get(TRACEPARENT).map(String::as_str), Some(HEADER));
    }

    #[test]
    fn malformed_or_empty_headers_carry_nothing() {
        for bad in [
            "",
            "garbage",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        ] {
            let headers = HashMap::from([(TRACEPARENT.to_string(), bad.to_string())]);
            assert_eq!(trace_id(&extract(&headers)), None, "{bad:?}");
        }
        let mut out = HashMap::new();
        inject(&Context::new(), &mut out);
        assert!(out.is_empty());
    }
}
