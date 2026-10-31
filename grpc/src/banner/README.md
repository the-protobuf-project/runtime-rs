# Startup banner

[Hybrid server](../../README.md)

After every enabled component starts, the lifecycle builds a `Summary` from
resolved options, registered service names, and actual agent endpoints. This
module renders the service name, build/version line, endpoint table, and
warnings. It opens no listeners and configures no services.

```rust
use grpc::{banner::{self, Summary}, options::Options};

fn main() {
    let options = Options { service_name: "shop".into(), version: "1.0.0".into(), ..Default::default() };
    banner::print(&options, &Summary::default());
}
```

`summary.rs` assembles rows; `print.rs` combines the full banner; `figure.rs`
uses `font.rs` and the bundled [speed font](../../assets/README.md); `table.rs`
implements hierarchical row merging and display-width-aware borders.
`style.rs` supplies formatting and terminal-only color. Redirected output
therefore stays free of color escape sequences.

Rendering deliberately follows Go's glyph concatenation and table output.
Keep the Go reference fixtures when changing layout; run from the repo root:

```bash
cargo test -p tpp-grpc --test banner_figure --test banner_table --test banner_full
cargo run -p tpp-grpc --example banner
```

The VCS build revision is embedded by [build.rs](../../build.rs), falling back
to `dev` outside a Git checkout. Closure-only gRPC registrations do not provide
service names for banner indexing; use `with_service` when names should appear.
