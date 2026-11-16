//! Environment variables as configuration keys.

use figment::providers::Env;
use uncased::Uncased;

/// The provider of every variable starting with prefix, less the reserved
/// ones (the profile and config-file variables, which steer the load rather
/// than set a key). The rest of a name is its key, lower-cased, with double
/// underscores between levels:
///
/// ```text
/// ACME_SERVER__GRPC__PORT      -> server.grpc.port
/// ACME_DATABASE__POSTGRES__DBNAME -> database.postgres.dbname
/// ```
///
/// A name without a double underscore reads every underscore as a level
/// (`ACME_SERVER_PORT` -> `server.port`), which cannot reach a key with an
/// underscore in it; that is what the double underscore is for.
pub(crate) fn provider(prefix: &str, reserved: Vec<String>) -> Env {
    let prefix = prefix.to_owned();
    Env::raw()
        .filter_map(move |name| {
            let upper = name.as_str().to_ascii_uppercase();
            let rest = upper.strip_prefix(&prefix)?;
            if rest.is_empty() || reserved.iter().any(|r| r == &upper) {
                return None;
            }
            Some(Uncased::from_owned(key(rest)))
        })
        .lowercase(true)
}

/// The key a variable name, less its prefix, sets.
pub(crate) fn key(rest: &str) -> String {
    let rest = rest.to_ascii_lowercase();
    if rest.contains("__") {
        rest.replace("__", ".")
    } else {
        rest.replace('_', ".")
    }
}

#[cfg(test)]
mod tests {
    use super::key;

    #[test]
    fn keys() {
        assert_eq!(key("SERVER__GRPC__PORT"), "server.grpc.port");
        assert_eq!(key("WEBHOOKS__SECRET_KEY"), "webhooks.secret_key");
        assert_eq!(key("SERVER_PORT"), "server.port");
    }
}
