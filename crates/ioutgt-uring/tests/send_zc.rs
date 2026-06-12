//! SENDMSG_ZC: two-CQE completion (send result, then notification),
//! notification-gated lifetime, orphan reclaim on drop.
//!
//! ZC sends require inet sockets (AF_UNIX is unsupported), so these
//! tests run over loopback TCP — where the kernel always falls back
//! to copying, which REPORT_USAGE reports; the CQE protocol is
//! identical either way.

use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::time::Duration;

use ioutgt_uring::{QueueRuntime, RingConfig, ops};

fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).unwrap();
    let (server, _) = listener.accept().unwrap();
    client.set_nodelay(true).unwrap();
    (client, server)
}

/// Two-iovec msghdr over caller-owned buffers.
fn gather_msg(a: &[u8], b: &[u8]) -> ([libc::iovec; 2], libc::msghdr) {
    let iovs = [
        libc::iovec {
            iov_base: a.as_ptr().cast_mut().cast(),
            iov_len: a.len(),
        },
        libc::iovec {
            iov_base: b.as_ptr().cast_mut().cast(),
            iov_len: b.len(),
        },
    ];
    // SAFETY: a zeroed msghdr is a valid value; iov fields set by caller.
    let msg: libc::msghdr = unsafe { std::mem::zeroed() };
    (iovs, msg)
}

#[test]
fn zc_send_two_cqes_and_reassembly() {
    let (client, server) = tcp_pair();
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    let reactor = rt.reactor().clone();

    rt.block_on(async move {
        let a = b"hdr:".to_vec();
        let b = vec![0xa5u8; 32 * 1024];
        let (iovs, mut msg) = gather_msg(&a, &b);
        msg.msg_iov = iovs.as_ptr().cast_mut();
        msg.msg_iovlen = 2;
        let total = a.len() + b.len();

        // SAFETY: msg, iovs, and both buffers outlive the notif await.
        let mut op = unsafe { ops::sendmsg_zc_raw(client.as_raw_fd(), &raw const msg) }.unwrap();
        let n = op.sent().await.unwrap() as usize;
        assert_eq!(n, total, "32K+4 fits the default loopback sndbuf");

        // Drain the peer so the skbs free and the notif fires.
        let mut got = Vec::new();
        let mut buf = vec![0u8; 8192].into_boxed_slice();
        while got.len() < total {
            let (res, b2) = ops::recv(server.as_raw_fd(), buf).unwrap().await;
            let r = res.unwrap() as usize;
            assert!(r > 0, "peer closed early");
            got.extend_from_slice(&b2[..r]);
            buf = b2;
        }
        assert_eq!(&got[..4], b"hdr:");
        assert!(got[4..].iter().all(|&x| x == 0xa5), "payload corrupted");

        let copied = op.into_notif().await;
        // Loopback degrades to copy; informational, not asserted.
        eprintln!("loopback ZC copied fallback: {copied}");
        assert_eq!(reactor.pending_ops(), 0, "notif must be terminal");
    });
}

#[test]
fn dropped_zc_notif_reclaims_via_orphan() {
    let (client, server) = tcp_pair();
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    let reactor = rt.reactor().clone();

    rt.block_on(async move {
        let a = b"x".to_vec();
        let b = vec![1u8; 4096];
        let (iovs, mut msg) = gather_msg(&a, &b);
        msg.msg_iov = iovs.as_ptr().cast_mut();
        msg.msg_iovlen = 2;

        // SAFETY: buffers stay alive until the drain below confirms
        // the terminal CQE has been reaped.
        let mut op = unsafe { ops::sendmsg_zc_raw(client.as_raw_fd(), &raw const msg) }.unwrap();
        op.sent().await.unwrap();
        drop(op.into_notif()); // orphan: reactor frees on the notif CQE

        let sink = vec![0u8; 8192].into_boxed_slice();
        let (res, _) = ops::recv(server.as_raw_fd(), sink).unwrap().await;
        assert!(res.unwrap() > 0);

        for _ in 0..500 {
            if reactor.pending_ops() == 0 {
                break;
            }
            ops::sleep(Duration::from_millis(2)).unwrap().await.unwrap();
        }
        assert_eq!(reactor.pending_ops(), 0, "orphaned ZC entry leaked");
    });
}

#[test]
fn dropped_zc_op_before_result_reclaims() {
    let (client, _server) = tcp_pair();
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    let reactor = rt.reactor().clone();

    rt.block_on(async move {
        let a = b"y".to_vec();
        let b = vec![2u8; 512];
        let (iovs, mut msg) = gather_msg(&a, &b);
        msg.msg_iov = iovs.as_ptr().cast_mut();
        msg.msg_iovlen = 2;

        // SAFETY: buffers stay alive until the drain below.
        let op = unsafe { ops::sendmsg_zc_raw(client.as_raw_fd(), &raw const msg) }.unwrap();
        drop(op); // never polled

        for _ in 0..500 {
            if reactor.pending_ops() == 0 {
                break;
            }
            ops::sleep(Duration::from_millis(2)).unwrap().await.unwrap();
        }
        assert_eq!(reactor.pending_ops(), 0, "never-polled ZC op leaked");
    });
}
