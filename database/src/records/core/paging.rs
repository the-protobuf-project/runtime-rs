//! Offset page tokens, and the read-one-past paging that skips a count. Go's `core/paging.go`.

/// The page size a backend uses when none is requested.
pub const DEFAULT_PAGE_SIZE: i64 = 50;

/// The total a backend reports when it skipped counting.
pub const NO_TOTAL: i64 = -1;

/// The requested page size, or the default.
pub fn page_size(requested: i32) -> i64 {
    if requested <= 0 {
        DEFAULT_PAGE_SIZE
    } else {
        i64::from(requested)
    }
}

/// The offset a token encodes; 0 for an empty or unreadable one. Go's `core.DecodeToken`.
pub fn decode_token(token: &str) -> i64 {
    token.parse::<i64>().ok().filter(|n| *n >= 0).unwrap_or(0)
}

/// The token after a page of `returned` rows at `offset`; empty once `total` is reached.
pub fn encode_token(offset: i64, returned: i64, total: i64) -> String {
    let next = offset + returned;
    if next >= total {
        String::new()
    } else {
        next.to_string()
    }
}

/// How many rows to read: one past the page when the total is skipped, to learn whether
/// another page exists.
pub fn fetch_limit(limit: i64, omit_total: bool) -> i64 {
    if omit_total { limit + 1 } else { limit }
}

/// The page out of `rows`, and the next token. Go's `core.TrimPage`.
pub fn trim_page<T>(
    mut rows: Vec<T>,
    offset: i64,
    limit: i64,
    total: i64,
    omit_total: bool,
) -> (Vec<T>, String) {
    if !omit_total {
        let token = encode_token(offset, rows.len() as i64, total);
        return (rows, token);
    }
    if rows.len() as i64 > limit {
        rows.truncate(limit as usize);
        return (rows, (offset + limit).to_string());
    }
    (rows, String::new())
}
