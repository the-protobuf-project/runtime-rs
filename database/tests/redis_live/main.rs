//! Live Redis verification of `database::redis`, every case of runtime-go's
//! `database/redis/redis_test.go` plus the Rust additions.
//!
//! Selected by the `live-tests` feature rather than marked ignored: once enabled, an
//! unreachable server is a failure, never a skip. Start the isolated service from the
//! repository root, run, then remove it:
//!
//! ```text
//! docker compose -p tpp-database-redis-live -f database/docker/compose.redis.live.yaml up -d --wait
//! cargo test -p tpp-database --features live-tests --test redis_live
//! docker compose -p tpp-database-redis-live -f database/docker/compose.redis.live.yaml down
//! ```
//!
//! `RUNTIME_DATABASE_REDIS_ADDRESS` (`host:port`) defaults to `127.0.0.1:16381`. Every test
//! writes under its own prefix and drops what it wrote; nothing flushes the server.

#[path = "../support/records.rs"]
mod records;

mod crud;
mod list;
mod schema;
mod support;
mod tenants;
mod unique;
