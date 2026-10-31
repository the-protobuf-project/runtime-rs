# Developer tools

[Workspace guide](../README.md) · [README generator](docgen/README.md)

`docgen` is the current tool in this directory. It is excluded from workspace
members and publication because it maintains repository documentation rather
than shipping a runtime library.

```bash
just docs
just docs-check
```

Cargo metadata and manifest descriptions produce the root README. The module
and folder READMEs are maintained beside their implementations; generated root
navigation links readers into them. Compileable API examples also live in
crate rustdoc. When adding tooling, document its inputs, outputs, side effects,
and invocation here and in its own folder.
