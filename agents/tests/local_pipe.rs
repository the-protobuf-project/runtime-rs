//! The shared local listener on a Windows named pipe: its owner can connect and exchange
//! bytes over each of several connections, and a name already held is refused.

#![cfg(windows)]

use agents::listen::{LocalAddr, LocalListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::windows::named_pipe::ClientOptions;

fn addr(test: &str) -> LocalAddr {
    LocalAddr::parse(&format!("pipe:tpp-agents-{test}-{}", std::process::id())).expect("a pipe")
}

#[tokio::test]
async fn the_owner_connects_to_each_instance_in_turn() {
    let addr = addr("echo");
    let LocalAddr::Pipe(name) = &addr else {
        unreachable!()
    };
    let mut listener = LocalListener::bind(&addr).expect("binds");
    for round in 0u8..3 {
        let mut client = ClientOptions::new()
            .open(name)
            .expect("the owner may open it");
        let mut served = listener.accept().await.expect("accepts");
        client.write_all(&[round]).await.expect("writes");
        let mut byte = [0u8; 1];
        served.read_exact(&mut byte).await.expect("reads");
        assert_eq!(byte, [round]);
    }
}

#[tokio::test]
async fn a_name_already_held_is_refused() {
    let addr = addr("held");
    let _first = LocalListener::bind(&addr).expect("binds");
    let second = LocalListener::bind(&addr).expect_err("refused");
    assert_eq!(second.kind(), std::io::ErrorKind::AddrInUse, "{second}");
}

#[test]
fn a_unix_socket_is_refused_on_windows() {
    let addr = LocalAddr::parse(r"unix:C:\tmp\a.sock").expect("parses");
    let refused = LocalListener::bind(&addr).expect_err("refused");
    assert_eq!(refused.kind(), std::io::ErrorKind::Unsupported);
}
