//! The summary table, against output captured from the Go runtime's `tablewriter`.

use grpc::banner::{Row, render_table};

fn sample() -> Vec<Row> {
    vec![
        Row::new("gRPC", "bookstore.v1.BookService", "0.0.0.0:50051", "TLS ✓"),
        Row::new("gRPC", "Health Check", "0.0.0.0:50051", "grpc.health.v1"),
        Row::new("gRPC", "Reflection", "0.0.0.0:50051", "grpcurl-compatible"),
        Row::new(
            "gRPC",
            "OpenTelemetry",
            "0.0.0.0:50051",
            "OTLP → localhost:12005",
        ),
        Row::new(
            "MCP",
            "Server 1  (streamable-http)",
            "0.0.0.0:8081",
            "/bookstore/v1/mcp",
        ),
        Row::new(
            "A2A",
            "Agent 1  (jsonrpc)",
            "0.0.0.0:8082",
            "http://0.0.0.0:8082/a2a  card /.well-known/agent-card.json",
        ),
        Row::new(
            "HTTP/1.1",
            "2 REST Routes  +  /health",
            "0.0.0.0:8080",
            "https",
        ),
        Row::new("HTTP/3 ⚗", "REST Gateway (QUIC)", "0.0.0.0:8081", "TLS 1.3"),
    ]
}

#[test]
fn the_table_matches_the_go_runtime_exactly() {
    let golden = include_str!("fixtures/table_golden.txt");
    assert_eq!(
        render_table(&sample()).trim_end(),
        golden.trim_end(),
        "summary table drifted from the Go runtime"
    );
}

#[test]
fn a_repeated_section_merges_into_the_one_above_it() {
    let table = render_table(&sample());
    // The four gRPC rows state the section once.
    assert_eq!(table.matches("gRPC ").count(), 1, "{table}");
    // And the rows beneath it continue the box rather than closing it.
    assert!(table.contains("│          ├"), "{table}");
}

#[test]
fn no_rows_renders_no_frame() {
    assert!(render_table(&[]).is_empty());
}

#[test]
fn a_wide_cell_widens_its_column_rather_than_wrapping() {
    let long = "a-very-long-detail-value-here";
    let table = render_table(&[Row::new("gRPC", "x", "y", long)]);
    assert!(
        table.lines().any(|line| line.contains(long)),
        "the value should be printed whole: {table}"
    );
}

#[test]
fn every_line_is_the_same_display_width() {
    use unicode_width::UnicodeWidthStr;
    let table = render_table(&sample());
    let widths: Vec<usize> = table.lines().map(UnicodeWidthStr::width).collect();
    let first = widths[0];
    assert!(
        widths.iter().all(|w| *w == first),
        "ragged table: widths {widths:?}"
    );
}
