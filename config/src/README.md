# Configuration loading

[Crate guide](../README.md)

`lib.rs` is the public facade. The load is in three pieces:

| File | What |
| --- | --- |
| `loader.rs` | `Loader`: the layers an application declares, merged in precedence order by `load` |
| `source.rs` | `Source` and `Format`: an embedded document or a file, TOML, YAML or JSON |
| `profile.rs` | `Profile`: whether the development profile applies (`<PREFIX>PROFILE`) |
| `env.rs` | Environment variables as keys: the prefix stripped, `__` between levels |
| `config.rs` | `Config`: typed reads (`extract`, `section`, the validated forms) over the merged layers |
| `validate.rs` | `Validate`: a section's own check, run at startup |
| `de.rs` | `comma_list`: a list from a sequence or one comma-separated string |
| `global.rs` | `install` / `get`: one configuration for the process |
| `error.rs` | `Error` |

Files are read with Figment's `file_exact`: where the path points, relative to
the working directory, never searched for in parent directories. A required
file that is missing fails the load with `Error::MissingFile` rather than
loading as empty.
