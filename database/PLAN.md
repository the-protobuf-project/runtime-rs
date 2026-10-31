# Database implementation plan

## Architecture

Follow runtime-go/database's descriptor and capability boundaries, using Rust
traits, Arc-owned clients, enums for values, and explicit capability wiring.
Resource descriptors belong to the portable core; backend SDK types stay in
drivers. Database selection returns a new handle and never changes a shared
client's selected database.

InfluxDB 3 is the first backend, using InfluxCommunity's influxdb3-client 0.3.
Its point identity is table + tag set + timestamp. Rewriting an identity merges
fields; it is not insert-only record CRUD. Expose a TimeSeries capability and
leave RecordStore absent. Unsupported capabilities return structured errors.
Add transactions, graph, migrations and watching when a real backend needs
them, rather than introducing placeholder implementations now.

## First slice

1. Resource/column descriptors, validated scalar records, structured errors.
2. Provider/DB selection, explicit RecordStore and TimeSeries capabilities.
3. TimeSeries writes, bounded half-open range queries, predicates, aggregation.
4. InfluxDB SDK adapter, explicit nanosecond timestamps, parameterized values,
   bounded operations and result sizes, caller-owned SDK client.
5. Serde Collection<T> with the same descriptors and operations.
6. Unit/transport tests, opt-in live test, runnable example and documentation.
7. Workspace registration, generated documentation, fmt/clippy/test checks.

## Contract and validation

- Batch writes are not transactions and may partly commit across SDK batches.
- No implicit server timestamps: retrying must preserve point identity.
- Validate all points before any network write. Reject unknown columns, wrong
  types, non-finite floats, invalid time ranges and unsafe limits.
- Queries use SQL parameters for values and quote descriptor identifiers.
- A range is bounded and ordered, not a snapshot or a complete unbounded scan.
- Selecting a database probes it but never creates it. Clients are Arc-owned;
  dropping a DB does not shut down other users of the client.
- Local tests establish encoding, query generation and adapter behavior. Only
  the opt-in live test establishes compatibility with a real InfluxDB server.

## Later backends

Implement RecordStore for SQL/document stores over the same Resource/Record
types. Add protobuf reflection as another bridge, not as a second storage path.
Introduce transactions/migrations/graph only alongside working implementations.
Preserve capability bundles when adding caching and telemetry decorators.

## File size rule

Every Rust source file, including documentation and tests, stays below 200 lines.

## Implemented and verified

- All seven first-slice steps are implemented.
- SDK 0.3.0, InfluxDB 3 Core 3.12.2 (latest `3-core` Docker image).
- HTTP/Arrow Flight fixture tests exercise real SDK transports.
- Real-server test passed: writes, duplicate-point rewrites, half-open ranges,
  typed reads (example), nanosecond decoding and SQL bucketed aggregation.
- Docker service stays running; credentials live only in ignored docker/.env.
- Per-file size test checks every Rust source, fixture and example with docs.
- Workspace tests and clippy passed; formatting, rustdoc warnings-as-errors,
  generated README freshness and diff whitespace checks passed.
- Final Compose service is healthy; live test passed again after recreation,
  confirming the database and credentials survive container replacement.

## Lifecycle and sustained workloads

- Database creation with explicit retention via an InfluxAdmin adapter. Core
  retention is immutable; selecting a database remains read-only.
- Explicit table deletion removes the schema and all observations. Core ignores
  the documented data_only flag; that flag is never sent. Selective row deletion
  is an independent, absent Core capability and returns Unsupported.
- Keyset pages ordered by time + the complete declared tag set; scoped opaque
  cursors reject mismatched databases/descriptors/queries. No snapshot claim.
- Pull-based SDK Arrow Flight streams with cancellation by Drop and an explicit
  complete-stream deadline. No task or channel buffers an entire query result.
- Bounded background writer: queue backpressure, count/timer/manual flush,
  admission vs durability distinguished, terminal failures reported, draining
  shutdown. Worker ownership survives cancellation of a close waiter.
- Unit/failure tests plus disposable live databases for retention, table data
  deletion, pagination, streams and sustained writes. Keep Compose running.

## Lifecycle verification results

- InfluxDB 3.12.2 live tests passed for code-only database creation, finite
  retention filtering, background admission/flush/drain, uncapped streaming,
  equal-time keyset paging in ascending/descending time order, and explicit
  whole-table/database deletion.
- Core API reality differs from the generated API page: infinite retention is
  expressed by omitting retention_period, not the string none. Data-only table
  deletion is Enterprise-only; Core ignores the parameter and drops schema.
  The adapter never sends data_only and never substitutes a table drop for a
  requested range. RowDeletion remains absent on Core.
- Failure tests cover stream stalls/breaks without replay, queue backpressure,
  cancelled sends/close waiters, terminal backend failures and batch deadlines.
- All InfluxDB examples are grouped in examples/influxdb. A single runnable
  workflow creates a disposable database, exercises the complete lifecycle,
  and cleans it up while leaving Docker running.

## Shared runtime observations

- The workspace telemetry crate provides injected, no-op-by-default OTel API
  handles; the linked telemetry-rs SDK remains the optional exporter owner.
- Provider/admin, all capability wrappers, stream lifetimes and ingestion
  lifecycle are observed. Capability availability survives wrapping/cloning.
- Queue leases balance even on failure; batches link bounded producer contexts.
- In-memory exporters verify all three signals, privacy, parentage, cancellation
  and exact-once completion. The actual SDK exported traces/metrics/logs to a
  local collector during the full InfluxDB example; no database token/name
  appeared in collector output.
- InfluxDB remains running; the opt-in telemetry Compose profile adds a local
  collector on configurable port 16009 without affecting the existing 6009
  service. Examples remain together in examples/influxdb.
