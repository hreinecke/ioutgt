//! Client-side `IORING_OP_CONNECT`: dial a std `TcpListener` from the reactor,
//! then round-trip a message over the connected socket.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use ioutgt_uring::{QueueRuntime, RingConfig, ops};

#[test]
fn connect_then_echo() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // A blocking peer that echoes one 4-byte message.
    let peer = std::thread::spawn(move || {
        let (mut sock, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4];
        sock.read_exact(&mut buf).unwrap();
        sock.write_all(&buf).unwrap();
    });

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        // SAFETY: a fresh TCP socket fd, exclusively owned.
        let fd = unsafe {
            let raw = libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
            assert!(raw >= 0, "socket()");
            OwnedFd::from_raw_fd(raw)
        };
        ops::connect(fd.as_raw_fd(), &addr).unwrap().await.unwrap();

        let msg = vec![0xde, 0xad, 0xbe, 0xef].into_boxed_slice();
        let (res, b) = ops::send(fd.as_raw_fd(), msg).unwrap().await;
        assert_eq!(res.unwrap() as usize, 4);
        let (res, b) = ops::recv(fd.as_raw_fd(), b).unwrap().await;
        assert_eq!(res.unwrap() as usize, 4);
        assert_eq!(&b[..], &[0xde, 0xad, 0xbe, 0xef]);
    });

    peer.join().unwrap();
}
