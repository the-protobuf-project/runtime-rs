# Configuration tests

[Crate guide](../README.md)

```bash
cargo test -p tpp-config
```

`loader.rs` drives the loader against real files and environment variables,
each test inside a `figment::Jail` (a temporary working directory, and
environment variables restored when it ends):

| Test | Checks |
| --- | --- |
| `every_format_parses_to_the_same_config` | TOML, YAML (`.yaml`, `.yml`) and JSON, as files and embedded, read into one typed config |
| `an_unknown_extension_is_refused` | `Error::UnsupportedFormat` |
| `env_overrides_files_override_the_profile_overrides_the_defaults` | Precedence, key by key |
| `the_config_variable_names_a_file` | `<PREFIX>CONFIG`, and that it is not itself a key |
| `the_development_profile` | Auto, dev, release, the file replacing the embedded profile, layers over it |
| `environment_variables_are_typed_keys` | Typed values, `__` levels, comma lists, other prefixes ignored, `without_env` |
| `lists_read_from_files_too` | `comma_list` over a TOML array |
| `bad_configuration_is_refused_naming_the_key` | `Validate`, a value of the wrong type, a file that does not parse |
| `optional_files_and_defaults` | `optional_file`, `section_or_default` |
| `global_installs_once` | `global::install`, `global::get` |
