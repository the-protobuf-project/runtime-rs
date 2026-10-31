# Banner preview program

[Hybrid server guide](../README.md) · [Banner source](../src/banner/README.md)

`banner.rs` constructs sample server options and startup summaries, then prints
banners for local inspection. It exercises rendering without starting any
network services or needing TLS/credentials.

```bash
cargo run -p tpp-grpc --example banner
cargo run -p tpp-grpc --example banner > /tmp/runtime-rs-banner.txt
```

Terminal output can use color; redirected output is plain text. Review section
merging, endpoint rows, service-name art, and warnings. Keep automated golden
checks alongside visual inspection:

```bash
cargo test -p tpp-grpc --test banner_figure --test banner_table --test banner_full
```

For a server that actually handles requests, use the runnable program in the
[HTTP surface guide](../src/http/README.md) or the integration tests' service
setup. This example is specifically a rendering preview.
