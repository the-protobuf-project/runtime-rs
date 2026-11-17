//! What every driver shares: the AIP-160 filter subset, offset page tokens, and the columns
//! a driver fills itself. Go's `database/core`.

mod filter;
mod managed;
mod paging;

pub use filter::{Clause, parse_filter};
pub use managed::{fill_managed, generate_id, has_managed};
pub use paging::{
    DEFAULT_PAGE_SIZE, NO_TOTAL, decode_token, encode_token, fetch_limit, page_size, trim_page,
};
