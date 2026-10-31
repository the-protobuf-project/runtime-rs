# Generated MCP compatibility fixture

[Agent tests](../README.md)

`counter_service_mcp.rs` is checked-in `protoc-gen-mcp` output from the upstream
counter example, used by `generated_mcp.rs`. It proves the runtime accepts the
generator's actual handler shape and proto-derived path.

```bash
cargo test -p tpp-agents --test generated_mcp
```

The test supplies the counter implementation, wraps the generated handler in
an MCP Service, starts a local runtime, and drives initialization/tool calls.
Generated code therefore has to compile and interoperate through the runtime.

To refresh, obtain the counter service output from the upstream location named
in the fixture header. Preserve generator output verbatim except for the
trailing standalone `serve_*` helpers, whose listener ownership is replaced
by this runtime. Record the generator version/provenance in that header and
rerun the target above. Do not hand-edit the fixture to accommodate runtime
changes; doing so would stop testing compatibility with real generated code.
