use std::collections::BTreeMap;

use crate::error::{Error, Result};
use crate::options::{UrlOptions, UrlScheme};

/// Constructs a full URL from [`UrlOptions`] using the path at `path_index`. Validates a
/// non-empty host, a non-empty paths list, and that `path_index` is in range (accepting a signed
/// index so an out-of-range negative index is a normal validation error, not a type-level
/// impossibility). Query parameters from `params` are appended when present, sorted by key to
/// match Go's `url.Values.Encode()` (which sorts keys alphabetically), for deterministic output.
pub fn build_full_url(url_options: &UrlOptions, path_index: i64) -> Result<String> {
    if url_options.host.is_empty() {
        return Err(Error::EmptyHost);
    }
    if url_options.paths.is_empty() {
        return Err(Error::EmptyPaths);
    }
    if path_index < 0 || path_index as usize >= url_options.paths.len() {
        return Err(Error::PathIndexOutOfBounds {
            index: path_index,
            len: url_options.paths.len(),
        });
    }

    let path = &url_options.paths[path_index as usize];
    let path = if path.starts_with('/') {
        path.clone()
    } else {
        format!("/{path}")
    };

    let mut url = ::url::Url::parse(&format!("{}://{}", url_options.scheme, url_options.host))
        .map_err(Error::UrlParse)?;
    url.set_path(&path);

    if !url_options.params.is_empty() {
        let sorted: BTreeMap<&String, &String> = url_options.params.iter().collect();
        let mut query = url.query_pairs_mut();
        query.clear();
        for (k, v) in sorted {
            query.append_pair(k, v);
        }
    }

    Ok(url.to_string())
}

/// Converts a parsed [`url::Url`] into [`UrlOptions`]: scheme, host, and a single path
/// (defaulting to `"/"`), with any query parameters copied into `params`. Lets generated clients
/// connect using the standard library's URL-parsing output directly.
pub fn url_options_from_std(u: &::url::Url) -> Result<UrlOptions> {
    let scheme: UrlScheme = u.scheme().parse()?;
    let host = u
        .host_str()
        .map(|h| match u.port() {
            Some(p) => format!("{h}:{p}"),
            None => h.to_string(),
        })
        .unwrap_or_default();
    let path = if u.path().is_empty() {
        "/".to_string()
    } else {
        u.path().to_string()
    };
    let params: std::collections::HashMap<String, String> = u
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();

    Ok(UrlOptions {
        scheme,
        host,
        paths: vec![path],
        params,
    })
}

/// Derives a ws/wss [`UrlOptions`] copy from an http/https [`UrlOptions`], used to open a
/// subscription transport against the same host and path.
pub fn websocket_url(input: &UrlOptions) -> UrlOptions {
    let mut out = input.clone();
    out.scheme = match input.scheme {
        UrlScheme::Https | UrlScheme::Wss => UrlScheme::Wss,
        _ => UrlScheme::Ws,
    };
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(paths: &[&str]) -> UrlOptions {
        UrlOptions {
            scheme: UrlScheme::Https,
            host: "example.com".to_string(),
            paths: paths.iter().map(|s| s.to_string()).collect(),
            params: Default::default(),
        }
    }

    #[test]
    fn builds_full_url() {
        let url = build_full_url(&opts(&["/graphql"]), 0).unwrap();
        assert_eq!(url, "https://example.com/graphql");
    }

    #[test]
    fn adds_leading_slash() {
        let url = build_full_url(&opts(&["graphql"]), 0).unwrap();
        assert_eq!(url, "https://example.com/graphql");
    }

    #[test]
    fn appends_sorted_query_params() {
        let mut o = opts(&["/graphql"]);
        o.params.insert("b".to_string(), "2".to_string());
        o.params.insert("a".to_string(), "1".to_string());
        let url = build_full_url(&o, 0).unwrap();
        assert_eq!(url, "https://example.com/graphql?a=1&b=2");
    }

    #[test]
    fn rejects_empty_host() {
        let mut o = opts(&["/graphql"]);
        o.host = String::new();
        assert!(matches!(build_full_url(&o, 0), Err(Error::EmptyHost)));
    }

    #[test]
    fn rejects_empty_paths() {
        let o = opts(&[]);
        assert!(matches!(build_full_url(&o, 0), Err(Error::EmptyPaths)));
    }

    #[test]
    fn rejects_out_of_bounds_index() {
        let o = opts(&["/a"]);
        assert!(matches!(
            build_full_url(&o, -1),
            Err(Error::PathIndexOutOfBounds { .. })
        ));
        assert!(matches!(
            build_full_url(&o, 5),
            Err(Error::PathIndexOutOfBounds { .. })
        ));
    }

    #[test]
    fn websocket_url_upgrades_https_to_wss() {
        let o = opts(&["/graphql"]);
        assert_eq!(websocket_url(&o).scheme, UrlScheme::Wss);
    }

    #[test]
    fn websocket_url_downgrades_http_to_ws() {
        let mut o = opts(&["/graphql"]);
        o.scheme = UrlScheme::Http;
        assert_eq!(websocket_url(&o).scheme, UrlScheme::Ws);
    }
}
