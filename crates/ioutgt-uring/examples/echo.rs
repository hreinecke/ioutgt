//! TCP echo server on the ioutgt-uring reactor — the syscall-rate fixture.
//!
//! Run:    cargo run --release --example echo -- 127.0.0.1:9999
//! Probe:  strace -c -p $(pidof echo)  while driving load, e.g.
//!         `nc 127.0.0.1 9999` or a small flood client; the
//!         io_uring_enter count must stay far below the message count.

use std::os::fd::{AsRawFd, OwnedFd};

use ioutgt_uring::{QueueRuntime, RingConfig, ops};

fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9999".into());
    let listener = std::net::TcpListener::bind(&addr)?;
    eprintln!("echo listening on {addr}");

    let rt = QueueRuntime::new(RingConfig::default())?;
    rt.block_on(async move {
        let mut incoming = ops::accept_multi(listener.as_raw_fd())?;
        while let Some(conn) = incoming.next().await {
            let conn: OwnedFd = conn?;
            tokio::task::spawn_local(async move {
                let fd = conn.as_raw_fd();
                let mut buf = vec![0u8; 64 * 1024].into_boxed_slice();
                loop {
                    let (res, b) = match ops::recv(fd, buf) {
                        Ok(op) => op.await,
                        Err(_) => return,
                    };
                    let n = match res {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n as usize,
                    };
                    // Echo back exactly n bytes.
                    let chunk = b[..n].to_vec().into_boxed_slice();
                    buf = b;
                    let (res, _) = match ops::send(fd, chunk) {
                        Ok(op) => op.await,
                        Err(_) => return,
                    };
                    if res.is_err() {
                        return;
                    }
                }
            });
        }
        Ok(())
    })
}
