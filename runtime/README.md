# tpp-runtime

The stable, single-import facade that generated GraphQL clients depend on. Re-exports the
essentials of [`tpp-network`](https://crates.io/crates/tpp-network) so generated code references
one crate instead of reaching into transport internals, and adds `Tx` for committing several
mutations as one atomic GraphQL batch. A Rust port of
[`runtime-go/network/runtime`](https://github.com/the-protobuf-project/runtime-go/tree/main/network/runtime).

## Install

```bash
cargo add tpp-runtime
```

The library crate is imported as `runtime` regardless of the published package name.

## Quick start

```rust,no_run
# async fn example() -> runtime::Result<()> {
let mut network = runtime::new_connection(runtime::ClientType::GraphQL)?;
network.with_opts(runtime::ConnectionOptions {
    url: runtime::UrlOptions {
        scheme: runtime::HTTP,
        host: "localhost:3280".to_string(),
        paths: vec!["/graphql".to_string()],
        params: Default::default(),
    },
    ..Default::default()
}).await?;
let gql = network.as_graphql()?;
# Ok(())
# }
```

### Atomic batched mutations

```rust,no_run
use runtime::{BatchOp, Tx};

# async fn example(gql: &runtime::GraphQLClient) -> runtime::Result<()> {
let mut tx = Tx::new(gql);
tx.add(BatchOp { field: "insertPromocode".into(), args: vec![], selection: "{ id }".into() });
tx.add(BatchOp { field: "insertBookingMoney".into(), args: vec![], selection: "{ id }".into() });
let results = tx.commit().await?; // results[0], results[1] — decoded in queued order
# Ok(())
# }
```

Unlike Go's `Tx.Commit` (which fills pre-registered result pointers via reflection and returns
only an error), `commit` returns the decoded results directly — Rust has no equivalent in-place
mutation across per-op result types. See the
[workspace README](https://github.com/the-protobuf-project/runtime-rs#differences-from-the-go-implementation)
for this and every other intentional Go-parity deviation.

Full crate documentation: `cargo doc --open -p tpp-runtime` (or
[docs.rs](https://docs.rs/tpp-runtime) once published).

## License

Apache-2.0
