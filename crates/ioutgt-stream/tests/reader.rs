//! Tests for `StreamReader` — the protocol-neutral buffered byte-source.
//!
//! Mechanics only: fill/consume windowing, EOF, and the direct-into-caller
//! `read_direct` path (fragment reassembly, short-on-close, per-completion
//! `on_chunk`). No protocol knowledge — that lives in the transport crates.

use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;

use ioutgt_stream::StreamReader;
use ioutgt_uring::{QueueRuntime, RingConfig};

/// Send `bytes` as one `libc::send` syscall on `fd`.
fn send_all(fd: i32, bytes: &[u8]) {
    // SAFETY: `bytes` is valid for `bytes.len()` readable bytes.
    let ret = unsafe { libc::send(fd, bytes.as_ptr().cast(), bytes.len(), 0) };
    assert_eq!(ret as usize, bytes.len(), "send failed: {ret}");
}

/// `fill` returns the recv'd window; `consume` advances within it without
/// issuing a new recv.
#[test]
fn fill_returns_window_then_consume_advances() {
    let (client, server) = UnixStream::pair().unwrap();
    let pattern: Vec<u8> = (0..16u8).collect();

    let writer = {
        let pattern = pattern.clone();
        thread::spawn(move || {
            send_all(client.as_raw_fd(), &pattern);
            // Hold the socket open until the reader is done.
            thread::sleep(Duration::from_millis(50));
            drop(client);
        })
    };

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        let mut reader = StreamReader::new(server.as_raw_fd(), 64);

        let window = reader.fill().await.unwrap();
        assert_eq!(window, &pattern[..], "first fill returns the whole send");

        reader.consume(8);
        // No new recv: the second fill returns the unconsumed suffix.
        let window = reader.fill().await.unwrap();
        assert_eq!(window, &pattern[8..], "fill returns the unconsumed tail");

        reader.consume(8);
        drop(server);
    });

    writer.join().unwrap();
}

/// `fill` returns an empty slice when the peer closes without sending.
#[test]
fn fill_empty_on_eof() {
    let (client, server) = UnixStream::pair().unwrap();
    let writer = thread::spawn(move || {
        // Close immediately → EOF on the server side.
        drop(client);
    });

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        let mut reader = StreamReader::new(server.as_raw_fd(), 64);
        let window = reader.fill().await.unwrap();
        assert!(window.is_empty(), "fill must report EOF as an empty window");
        drop(server);
    });

    writer.join().unwrap();
}

/// After the window is fully consumed, the next `fill` issues a fresh recv
/// and returns the next bytes.
#[test]
fn fill_refills_after_full_consume() {
    let (client, server) = UnixStream::pair().unwrap();
    let writer = thread::spawn(move || {
        send_all(client.as_raw_fd(), b"AAAA");
        // Separate the two sends so the first recv returns only "AAAA".
        thread::sleep(Duration::from_millis(20));
        send_all(client.as_raw_fd(), b"BBBB");
        thread::sleep(Duration::from_millis(50));
        drop(client);
    });

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        let mut reader = StreamReader::new(server.as_raw_fd(), 64);

        let window = reader.fill().await.unwrap();
        assert_eq!(window, b"AAAA", "first window");
        let len = window.len();
        reader.consume(len);

        let window = reader.fill().await.unwrap();
        assert_eq!(window, b"BBBB", "refilled window after full consume");
        let len = window.len();
        reader.consume(len);

        drop(server);
    });

    writer.join().unwrap();
}

/// `read_direct` reassembles N bytes sent as many small fragments, and
/// `on_chunk` sees exactly those bytes in order.
#[test]
fn read_direct_assembles_fragments() {
    const N: usize = 64 * 1024;
    const CHUNK: usize = 4 * 1024;

    let (client, server) = UnixStream::pair().unwrap();
    #[allow(clippy::cast_possible_truncation)]
    let expected: Vec<u8> = (0..N).map(|i| (i & 0xFF) as u8).collect();

    let writer = {
        let expected = expected.clone();
        thread::spawn(move || {
            let mut sent = 0;
            while sent < N {
                let end = (sent + CHUNK).min(N);
                send_all(client.as_raw_fd(), &expected[sent..end]);
                sent = end;
                thread::sleep(Duration::from_millis(2));
            }
            drop(client);
        })
    };

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        // Small scratch buffer: read_direct does not touch it.
        let mut reader = StreamReader::new(server.as_raw_fd(), 64);
        let mut dst = vec![0u8; N];
        let mut seen: Vec<u8> = Vec::new();

        // SAFETY: `dst` is valid for N bytes and outlives the awaited op
        // (it is on this async block's stack; the await resolves first).
        #[allow(clippy::cast_possible_truncation)]
        let n = unsafe {
            reader
                .read_direct(dst.as_mut_ptr(), N as u32, |c| seen.extend_from_slice(c))
                .await
                .unwrap()
        } as usize;

        assert_eq!(n, N, "read_direct returned {n} instead of {N}");
        assert_eq!(dst, expected, "reassembled bytes do not match");
        assert_eq!(seen, expected, "on_chunk did not see every byte in order");

        drop(server);
    });

    writer.join().unwrap();
}

/// When the writer sends a partial payload then closes, `read_direct`
/// returns short and `on_chunk` saw exactly the partial bytes.
#[test]
fn read_direct_short_on_close() {
    const N: usize = 64 * 1024;
    const PARTIAL: usize = 3 * 1024;

    let (client, server) = UnixStream::pair().unwrap();
    let writer = thread::spawn(move || {
        let data = vec![0xABu8; PARTIAL];
        send_all(client.as_raw_fd(), &data);
        // Drop client → EOF on the server side.
    });

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        let mut reader = StreamReader::new(server.as_raw_fd(), 64);
        let mut dst = vec![0u8; N];
        let mut seen: Vec<u8> = Vec::new();

        // SAFETY: `dst` is valid for N bytes and outlives the awaited op.
        #[allow(clippy::cast_possible_truncation)]
        let n = unsafe {
            reader
                .read_direct(dst.as_mut_ptr(), N as u32, |c| seen.extend_from_slice(c))
                .await
                .unwrap()
        } as usize;

        assert_eq!(n, PARTIAL, "expected short return of {PARTIAL}, got {n}");
        assert_eq!(seen.len(), PARTIAL, "on_chunk saw {} bytes", seen.len());
        assert!(seen.iter().all(|&b| b == 0xAB), "partial bytes corrupted");

        drop(server);
    });

    writer.join().unwrap();
}
