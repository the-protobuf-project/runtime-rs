//! A loaded configuration and the typed reads over it.

use std::path::{Path, PathBuf};

use figment::Figment;
use serde::de::DeserializeOwned;

use crate::{Error, Profile, Validate};

/// A loaded configuration: every layer merged, ready to be read into the
/// application's types.
#[derive(Clone, Debug)]
pub struct Config {
    pub(crate) figment: Figment,
    pub(crate) profile: Profile,
    pub(crate) dev_applied: bool,
    pub(crate) files: Vec<PathBuf>,
}

impl Config {
    /// The whole configuration as T.
    pub fn extract<T: DeserializeOwned>(&self) -> Result<T, Error> {
        Ok(self.figment.extract()?)
    }

    /// The whole configuration as T, which then checks itself.
    pub fn extract_validated<T: DeserializeOwned + Validate>(&self) -> Result<T, Error> {
        let out: T = self.extract()?;
        out.validate().map_err(Error::Invalid)?;
        Ok(out)
    }

    /// The section at key -- dotted, `"engine"` or `"search.opensearch"` -- as
    /// T: how business logic reads a section the application declared on its
    /// own, where it is used.
    pub fn section<T: DeserializeOwned>(&self, key: &str) -> Result<T, Error> {
        Ok(self.figment.extract_inner(key)?)
    }

    /// The section at key as T, which then checks itself.
    pub fn section_validated<T: DeserializeOwned + Validate>(&self, key: &str) -> Result<T, Error> {
        let out: T = self.section(key)?;
        out.validate().map_err(Error::Invalid)?;
        Ok(out)
    }

    /// The section at key as T, or T's default when nothing sets it.
    pub fn section_or_default<T: DeserializeOwned + Default>(&self, key: &str) -> Result<T, Error> {
        match self.contains(key) {
            true => self.section(key),
            false => Ok(T::default()),
        }
    }

    /// Whether any layer sets key.
    pub fn contains(&self, key: &str) -> bool {
        self.figment.contains(key)
    }

    /// The profile the load asked for.
    pub fn profile(&self) -> Profile {
        self.profile
    }

    /// Whether the development profile was layered in.
    pub fn is_dev(&self) -> bool {
        self.dev_applied
    }

    /// The files the load read, in the order it layered them.
    pub fn files(&self) -> impl Iterator<Item = &Path> {
        self.files.iter().map(PathBuf::as_path)
    }

    /// The merged layers, for what the typed reads do not cover: the source
    /// a value came from ([`Figment::find_metadata`]), or a provider merged
    /// over the whole.
    pub fn figment(&self) -> &Figment {
        &self.figment
    }

    /// The merged layers, owned.
    pub fn into_figment(self) -> Figment {
        self.figment
    }
}
