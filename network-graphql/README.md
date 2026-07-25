# tpp-network-graphql

Predicate/filter/query-builder helpers for generated GraphQL CRUD clients: `where`-clause
predicates, typed column handles, an update-patch three-state `Nullable<T>`, cursor pagination,
and generic CRUD handler traits. A Rust port of
[`runtime-go/network/graphql`](https://github.com/the-protobuf-project/runtime-go/tree/main/network/graphql).
Has no dependency on the sibling [`tpp-network`](https://crates.io/crates/tpp-network) crate —
this is a standalone toolkit for whatever code generator emits your resource types.

## Install

```bash
cargo add tpp-network-graphql
```

The library crate is imported as `network_graphql` regardless of the published package name.

## Quick start

```rust
use network_graphql::{and, Nullable, Predicate, StringField};

// A generated resource module would expose typed column handles like this:
let name = StringField { col: "displayName" };
let email = StringField { col: "email" };

// Build a filter predicate for a `where` argument:
let filter: Predicate = and(&[
    name.ilike("%rick%"),
    email.is_null(false),
]);

// A masked update patch distinguishes "leave unchanged" from "clear to null" from "set a value":
struct UpdateInput {
    display_name: Nullable<String>,
}
let patch = UpdateInput { display_name: Nullable::value("Rick Sanchez".to_string()) };
assert!(patch.display_name.is_set());
```

Generated patch structs implement the `ColumnPatch` trait (see its doc example) to turn themselves
into a Hasura-style `{jsonName: {"set": value}}` update-columns map.

## What's here

| Module | Provides |
|---|---|
| `predicate` | `Predicate`, `and`/`or`/`not`/`relation` |
| `fields_string` / `fields_number` / `fields_other` | `StringField`, `Int64Field`, `FloatField`, `BoolField`, `JSONField`, `EnumField<E>` |
| `nullable` | `Nullable<T>` (unset / null / value) |
| `columns` | `OrderBy`, `OrderTerm`, `ColumnPatch` |
| `keyset` | Cursor pagination (`after`) |
| `requests` | `ListRequest`, `CreateRequest`, `UpdateRequest`, `DeleteRequest` builders |
| `handlers` | `QueryHandler<M>`, `MutationHandler<C, U, IR, UR, DR>` |
| `scalars` | `Variable`, `Int64`, `Bigdecimal` |

Full crate documentation: `cargo doc --open -p tpp-network-graphql` (or
[docs.rs](https://docs.rs/tpp-network-graphql) once published).

## License

Apache-2.0
