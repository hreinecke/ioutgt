//! Concurrent echo over a socketpair: exercises recv/send wake paths and
//! task interleaving on one reactor.

use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;

use ioutgt_uring::{QueueRuntime, RingConfig, ops};

const MSG_LEN: usize = 64;
const ROUNDS: usize = 200;

#[test]
fn socketpair_echo() {
    let (client, server) = UnixStream::pair().unwrap();
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();

    rt.block_on(async move {
        let server_fd = server.as_raw_fd();
        let echo = tokio::task::spawn_local(async move {
            // Keep the stream alive inside the task.
            let _server = server;
            let mut buf = vec![0u8; MSG_LEN].into_boxed_slice();
            let mut echoed = 0usize;
            loop {
                let (res, b) = ops::recv(server_fd, buf).unwrap().await;
                let n = res.unwrap() as usize;
                if n == 0 {
                    break; // client closed
                }
                assert_eq!(n, MSG_LEN);
                let (res, b) = ops::send(server_fd, b).unwrap().await;
                assert_eq!(res.unwrap() as usize, MSG_LEN);
                buf = b;
                echoed += 1;
            }
            echoed
        });

        let client_fd = client.as_raw_fd();
        let mut buf = vec![0u8; MSG_LEN].into_boxed_slice();
        for round in 0..ROUNDS {
            #[allow(clippy::cast_possible_truncation)]
            buf.iter_mut()
                .enumerate()
                .for_each(|(i, b)| *b = (round + i) as u8);
            let expect = buf.clone();
            let (res, b) = ops::send(client_fd, buf).unwrap().await;
            assert_eq!(res.unwrap() as usize, MSG_LEN);
            let (res, b) = ops::recv(client_fd, b).unwrap().await;
            assert_eq!(res.unwrap() as usize, MSG_LEN);
            assert_eq!(b, expect, "echo corrupted in round {round}");
            buf = b;
        }
        drop(client); // EOF for the server task
        assert_eq!(echo.await.unwrap(), ROUNDS);
    });
}

#[test]
fn vectored_send_reassembles() {
    let (client, server) = UnixStream::pair().unwrap();
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();

    rt.block_on(async move {
        let header: Box<[u8]> = b"hdr:".to_vec().into_boxed_slice();
        let payload: Box<[u8]> = b"payload-bytes".to_vec().into_boxed_slice();
        let total = header.len() + payload.len();

        let (res, _bufs) = ops::send_vectored(client.as_raw_fd(), header, payload)
            .unwrap()
            .await;
        assert_eq!(res.unwrap() as usize, total);

        let buf = vec![0u8; total].into_boxed_slice();
        let (res, buf) = ops::recv(server.as_raw_fd(), buf).unwrap().await;
        assert_eq!(res.unwrap() as usize, total);
        assert_eq!(&buf[..], b"hdr:payload-bytes");
    });
}
