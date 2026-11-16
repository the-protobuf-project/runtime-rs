//! Where one layer of configuration comes from, and in what format.

use std::borrow::Cow;
use std::path::{Path, PathBuf};

use figment::Figment;
use figment::providers::{Format as _, Json, Toml, Yaml};

use crate::Error;

/// The formats a configuration layer may be written in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    /// TOML (`.toml`).
    Toml,
    /// YAML (`.yaml`, `.yml`).
    Yaml,
    /// JSON (`.json`).
    Json,
}

impl Format {
    /// The format a path's extension names; an error for any other extension.
    pub fn from_path(path: &Path) -> Result<Format, Error> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase);
        match ext.as_deref() {
            Some("toml") => Ok(Format::Toml),
            Some("yaml" | "yml") => Ok(Format::Yaml),
            Some("json") => Ok(Format::Json),
            _ => Err(Error::UnsupportedFormat(path.to_path_buf())),
        }
    }
}

/// One layer of configuration: a document compiled into the binary, or a
/// file read at load time.
#[derive(Clone, Debug)]
pub struct Source {
    format: Format,
    kind: Kind,
}

#[derive(Clone, Debug)]
enum Kind {
    Text(Cow<'static, str>),
    File { path: PathBuf, required: bool },
}

impl Source {
    /// A TOML document, typically `include_str!`'d into the binary.
    pub fn toml(text: impl Into<Cow<'static, str>>) -> Source {
        Source::text(Format::Toml, text)
    }

    /// A YAML document.
    pub fn yaml(text: impl Into<Cow<'static, str>>) -> Source {
        Source::text(Format::Yaml, text)
    }

    /// A JSON document.
    pub fn json(text: impl Into<Cow<'static, str>>) -> Source {
        Source::text(Format::Json, text)
    }

    /// A document in format.
    pub fn text(format: Format, text: impl Into<Cow<'static, str>>) -> Source {
        Source {
            format,
            kind: Kind::Text(text.into()),
        }
    }

    /// The file at path, its format named by its extension. Loading fails
    /// when it does not exist.
    pub fn file(path: impl Into<PathBuf>) -> Result<Source, Error> {
        Source::file_with(path, true)
    }

    /// The file at path when it exists; nothing when it does not.
    pub fn optional_file(path: impl Into<PathBuf>) -> Result<Source, Error> {
        Source::file_with(path, false)
    }

    fn file_with(path: impl Into<PathBuf>, required: bool) -> Result<Source, Error> {
        let path = path.into();
        Ok(Source {
            format: Format::from_path(&path)?,
            kind: Kind::File { path, required },
        })
    }

    /// The source's format.
    pub fn format(&self) -> Format {
        self.format
    }

    /// The file the source reads, `None` for an embedded document.
    pub fn path(&self) -> Option<&Path> {
        match &self.kind {
            Kind::File { path, .. } => Some(path),
            Kind::Text(_) => None,
        }
    }

    /// Merges the source into figment, recording a file it read in loaded.
    /// A file is read exactly where its path points -- relative to the
    /// working directory, never searched for in its parents.
    pub(crate) fn merge_into(
        &self,
        figment: Figment,
        loaded: &mut Vec<PathBuf>,
    ) -> Result<Figment, Error> {
        match &self.kind {
            Kind::Text(text) => Ok(match self.format {
                Format::Toml => figment.merge(Toml::string(text)),
                Format::Yaml => figment.merge(Yaml::string(text)),
                Format::Json => figment.merge(Json::string(text)),
            }),
            Kind::File { path, required } => {
                if !path.is_file() {
                    return match required {
                        true => Err(Error::MissingFile(path.clone())),
                        false => Ok(figment),
                    };
                }
                loaded.push(path.clone());
                Ok(match self.format {
                    Format::Toml => figment.merge(Toml::file_exact(path)),
                    Format::Yaml => figment.merge(Yaml::file_exact(path)),
                    Format::Json => figment.merge(Json::file_exact(path)),
                })
            }
        }
    }
}
