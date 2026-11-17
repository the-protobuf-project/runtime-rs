//! What every driver shares: AIP-160 filters ([`filter`]), offset page tokens, and the
//! columns a driver fills itself. Go's `database/core`.

mod coerce;
mod conjunction;
pub mod filter;
mod managed;
mod order;
mod paging;

pub use coerce::coerce;
pub use conjunction::{Clause, parse_filter};
pub use managed::{fill_managed, generate_id, has_managed};
pub use order::order_by;
pub use paging::{
    DEFAULT_PAGE_SIZE, NO_TOTAL, decode_token, encode_token, fetch_limit, page_size, trim_page,
};
