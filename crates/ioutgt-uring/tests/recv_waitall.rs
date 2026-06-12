//! Tests for `ops::recv_raw_waitall` — MSG_WAITALL recv into caller-managed
//! memory.

use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;

use ioutgt_uring::{QueueRuntime, RingConfig, ops};

/// Single `recv_raw_waitall` for N bytes reassembles N bytes sent as many
/// small fragments with sleeps between them.
///
/// Writer sends 64 KiB in 4 KiB chunks with short delays; a single
/// WAITALL recv must complete with exactly 64 KiB, not the first chunk.
#[test]
fn waitall_assembles_fragments() {
    const N: usize = 64 * 1024; // 64 KiB
    const CHUNK: usize = 4 * 1024; // 4 KiB per write

    let (client, server) = UnixStream::pair().unwrap();

    // Writer thread: send N bytes in CHUNK-sized pieces with small sleeps.
    let writer = thread::spawn(move || {
        #[allow(clippy::cast_possible_truncation)]
        let data: Vec<u8> = (0..N).map(|i| (i & 0xFF) as u8).collect();
        let mut sent = 0;
        while sent < N {
            let end = (sent + CHUNK).min(N);
            // libc::send so we can be sure each chunk is a separate syscall.
            // SAFETY: slice pointer is valid for (end-sent) bytes.
            let ret = unsafe {
                libc::send(
                    client.as_raw_fd(),
                    data[sent..end].as_ptr().cast(),
                    end - sent,
                    0,
                )
            };
            assert!(ret > 0, "send failed: {ret}");
            sent += ret as usize;
            thread::sleep(Duration::from_millis(2));
        }
        // client drops here, sending EOF
    });

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        let server_fd = server.as_raw_fd();
        // Buffer outlives the awaited op trivially: it is on the stack of
        // this async block, which does not return until the await resolves.
        let mut buf = vec![0u8; N];

        // SAFETY: buf is valid for N bytes and outlives the terminal CQE
        // because we await inline before buf goes out of scope.
        #[allow(clippy::cast_possible_truncation)]
        let n = unsafe { ops::recv_raw_waitall(server_fd, buf.as_mut_ptr(), N as u32) }
            .unwrap()
            .await
            .unwrap() as usize;

        // MSG_WAITALL must hold the op until all N bytes arrive.
        assert_eq!(n, N, "recv_raw_waitall returned {n} instead of {N}");

        #[allow(clippy::cast_possible_truncation)]
        let expected: Vec<u8> = (0..N).map(|i| (i & 0xFF) as u8).collect();
        assert_eq!(buf, expected, "reassembled bytes do not match");

        // Keep server alive until here so the socket fd remains open.
        drop(server);
    });

    writer.join().unwrap();
}

/// When the writer sends a partial payload and then closes, `recv_raw_waitall`
/// returns short (the partial length); a subsequent recv returns 0 (EOF).
#[test]
fn waitall_short_on_close() {
    const N: usize = 64 * 1024; // what we ask for
    const PARTIAL: usize = 3 * 1024; // what actually arrives

    let (client, server) = UnixStream::pair().unwrap();

    // Writer thread: send a partial chunk then close.
    let writer = thread::spawn(move || {
        let data: Vec<u8> = vec![0xABu8; PARTIAL];
        // SAFETY: slice pointer is valid for PARTIAL bytes.
        let ret = unsafe { libc::send(client.as_raw_fd(), data.as_ptr().cast(), PARTIAL, 0) };
        assert_eq!(ret as usize, PARTIAL, "partial send failed: {ret}");
        // Drop client → peer hangup → EOF on server side.
    });

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        let server_fd = server.as_raw_fd();

        // First recv: asks for N, gets PARTIAL (short return on EOF).
        let mut buf = vec![0u8; N];
        // SAFETY: buf is valid for N bytes and outlives the terminal CQE
        // because we await inline before buf goes out of scope.
        #[allow(clippy::cast_possible_truncation)]
        let n = unsafe { ops::recv_raw_waitall(server_fd, buf.as_mut_ptr(), N as u32) }
            .unwrap()
            .await
            .unwrap() as usize;

        assert_eq!(n, PARTIAL, "expected short return of {PARTIAL}, got {n}");
        assert!(
            buf[..PARTIAL].iter().all(|&b| b == 0xAB),
            "partial bytes corrupted"
        );

        // Second recv: should return 0 (EOF).
        let eof_buf = vec![0u8; 64].into_boxed_slice();
        let (res, _) = ops::recv(server_fd, eof_buf).unwrap().await;
        assert_eq!(res.unwrap(), 0, "expected EOF (0), got non-zero");

        drop(server);
    });

    writer.join().unwrap();
}
