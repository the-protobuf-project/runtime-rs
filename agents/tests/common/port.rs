//! Ports for tests that must name one before their server starts.
//!
//! Binding port 0 to find a free port and then releasing it races: the operating system can
//! hand that port to the next port-0 bind in this process, another test's server, before
//! this test binds it again. These ports come from below every platform's ephemeral range,
//! which a port-0 bind is never given, and each one is handed out once per process.

use std::net::TcpListener;
use std::sync::atomic::{AtomicU16, Ordering};

/// The range ports are drawn from. Linux's ephemeral range starts at 32768; macOS's and
/// Windows' at 49152.
const FIRST: u16 = 20_000;
const LAST: u16 = 32_000;

static NEXT: AtomicU16 = AtomicU16::new(0);

/// A port nothing is listening on and no other test in this process will be given.
pub fn free_port() -> u16 {
    let span = LAST - FIRST;
    // Start each process somewhere else in the range, so two test binaries run side by side
    // do not walk the same ports.
    let start = (std::process::id() % u32::from(span)) as u16;
    loop {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        assert!(n < span, "every test port in {FIRST}..{LAST} is taken");
        let port = FIRST + (start + n) % span;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}
