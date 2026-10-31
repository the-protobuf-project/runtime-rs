//! Generated descriptors looked up by message name.

use super::{Error, Resource, Result};
use std::collections::BTreeMap;

/// Every resource a program knows, by message name. Generated code builds one
/// from all of its descriptors, as runtime-go's `NewRegistry(Resources...)`.
#[derive(Clone, Debug, Default)]
pub struct Registry {
    resources: BTreeMap<String, Resource>,
}

impl Registry {
    /// Indexes resources by name. A later resource with a repeated name wins,
    /// so a caller can override one generated descriptor.
    pub fn new(resources: impl IntoIterator<Item = Resource>) -> Self {
        Self {
            resources: resources
                .into_iter()
                .map(|r| (r.name().to_owned(), r))
                .collect(),
        }
    }

    /// The resource with this message name.
    pub fn resource(&self, name: &str) -> Result<&Resource> {
        self.resources
            .get(name)
            .ok_or_else(|| Error::Invalid(format!("no resource named {name}")))
    }

    /// Every registered name, sorted.
    pub fn names(&self) -> Vec<&str> {
        self.resources.keys().map(String::as_str).collect()
    }
}
