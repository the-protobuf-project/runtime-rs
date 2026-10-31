# Embedded banner font

[Banner implementation](../src/banner/README.md)

`speed.flf` is the FIGlet font embedded by the banner renderer. The font is
read at compile time, so deployed binaries do not need this directory beside
them. It supplies glyph data only; server configuration and listeners are
unrelated to this asset.

```bash
cargo run -p tpp-grpc --example banner
cargo test -p tpp-grpc --test banner_figure
```

The custom reader in `src/banner/font.rs` follows Go's go-figure glyph
concatenation rather than FIGlet smushing. After replacing/changing font data,
review rendered output and the Go-derived fixture in `tests/fixtures`; avoid
updating that reference merely to hide a rendering difference. Preserve the
font's embedded attribution when redistributing it.
