//! Where a resource's record entries live, and how a name selects the namespace.

use crate::{Error, Result};

/// One part of a namespace: a literal decided at generation time, or the value of a parent
/// segment of the resource pattern, read from the record's name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Part {
    /// A fixed segment.
    Literal(&'static str),
    /// The value the record's name holds for this pattern variable (`author` in
    /// `authors/{author}/books/{book}`).
    Segment(&'static str),
}

/// Where one resource's record entries live. Generated per cached resource, and named by
/// other resources' [`Edge`]s.
#[derive(Debug)]
pub struct Keyspace {
    /// The proto message, for diagnostics.
    pub resource: &'static str,
    /// The message's short name, for the operation names reported to the hooks
    /// (`invalidate Author`), as Go spells them.
    pub model: &'static str,
    /// The AIP resource pattern the name is read against.
    pub pattern: &'static str,
    /// The namespace, joined with `.`.
    pub namespace: &'static [Part],
}

impl Keyspace {
    /// The namespace the record named `name` is cached under.
    ///
    /// Refuses a name not of the pattern, and an empty segment value or one containing `:`:
    /// the first would collapse every parent into one namespace, the second let two
    /// spellings address one keyspace.
    pub fn namespace(&self, name: &str) -> Result<String> {
        let vars = self.bind(name).ok_or_else(|| {
            Error::Invalid(format!(
                "cache: {}: {name:?} is not a name of the pattern {:?}",
                self.resource, self.pattern
            ))
        })?;
        let mut parts = Vec::with_capacity(self.namespace.len());
        for part in self.namespace {
            match part {
                Part::Literal(lit) => parts.push(*lit),
                Part::Segment(var) => {
                    let value = vars
                        .iter()
                        .find(|(v, _)| v == var)
                        .map(|(_, value)| *value)
                        .unwrap_or_default();
                    if value.is_empty() || value.contains(':') {
                        return Err(Error::Invalid(format!(
                            "cache: {}: the {{{var}}} segment of {name:?} cannot scope a namespace",
                            self.resource
                        )));
                    }
                    parts.push(value);
                }
            }
        }
        Ok(parts.join("."))
    }

    /// The namespace when no part depends on the name.
    pub(super) fn fixed(&self) -> Option<String> {
        self.namespace
            .iter()
            .map(|part| match part {
                Part::Literal(lit) => Some(*lit),
                Part::Segment(_) => None,
            })
            .collect::<Option<Vec<_>>>()
            .map(|parts| parts.join("."))
    }

    /// The pattern's variables and the values `name` holds for them, or `None` when `name`
    /// is not of the pattern.
    pub(super) fn bind<'n>(&self, name: &'n str) -> Option<Vec<(&'static str, &'n str)>> {
        let pattern: Vec<&'static str> = self.pattern.split('/').collect();
        let segs: Vec<&str> = name.split('/').collect();
        if pattern.len() != segs.len() {
            return None;
        }
        let mut out = Vec::new();
        for (p, s) in pattern.into_iter().zip(segs) {
            match p.strip_prefix('{').and_then(|p| p.strip_suffix('}')) {
                Some(var) if !s.is_empty() => out.push((var, s)),
                Some(_) => return None,
                None if p == s => {}
                None => return None,
            }
        }
        Some(out)
    }

    /// The name an edge's reference value addresses: the value itself when it is a name of
    /// this pattern, else, for a pattern with one variable, the value as that variable.
    pub(super) fn resolve(&self, value: &str) -> Option<String> {
        if self.bind(value).is_some() {
            return Some(value.to_owned());
        }
        let vars: Vec<&str> = self
            .pattern
            .split('/')
            .filter(|s| s.starts_with('{'))
            .collect();
        if vars.len() != 1 || value.is_empty() || value.contains('/') {
            return None;
        }
        Some(self.pattern.replacen(vars[0], value, 1))
    }
}
