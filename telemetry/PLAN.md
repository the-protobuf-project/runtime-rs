# Runtime telemetry

Use OpenTelemetry API handles as the shared contract; do not duplicate exporter
configuration or shutdown owned by the application and the telemetry-rs SDK.
Default wiring is no-op and does not consult/install global providers.

- Explicit injected meter/tracer/log sink, plus an optional adapter to the
  linked telemetry-rs SDK pinned to its inspected revision.
- One observer per component; instruments created once and reused.
- Operation guards report completion, error, panic or cancellation exactly once.
- Async context attaches per Future poll, never across an await with a thread
  guard. Prefer current OTel context, with a tracing-layer parent bridge.
- Bounded component/backend/operation/outcome labels; no user input, tenant
  names, SQL, tokens, record values or raw error strings.
- Database wrappers preserve every optional capability and avoid double wrapping.
- Provider selection/admin, CRUD, series, paging, stream lifetime and ingestion
  admission/flush/close/batches/queue depth are covered. Multi-producer batches
  link bounded caller span contexts rather than assigning an arbitrary parent.
- In-memory SDK exporters verify all three signals and correlation. Live Influx
  workflow retains its single examples folder. Every new source file <200 lines.
