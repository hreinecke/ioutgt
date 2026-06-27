//! File read/write/fsync/fallocate through the ring.

use std::os::fd::AsRawFd;

use ioutgt_uring::{QueueRuntime, RingConfig, ops};

#[test]
fn write_fsync_read_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let fd = file.as_raw_fd();

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        let data: Box<[u8]> = b"hello ioutgt".to_vec().into_boxed_slice();
        let (res, _) = ops::write_at(fd, data, 0).unwrap().await;
        assert_eq!(res.unwrap(), 12);

        ops::fsync(fd, false).unwrap().await.unwrap();

        let buf = vec![0u8; 12].into_boxed_slice();
        let (res, buf) = ops::read_at(fd, buf, 0).unwrap().await;
        assert_eq!(res.unwrap(), 12);
        assert_eq!(&buf[..], b"hello ioutgt");
    });
}

#[test]
fn writev_readv_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vdata");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let fd = file.as_raw_fd();

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        // Two non-adjacent source buffers written as one vectored op land
        // back to back on disk, in order.
        let a = vec![0x11u8; 4096];
        let b = vec![0x22u8; 2048];
        let iov = [
            libc::iovec {
                iov_base: a.as_ptr().cast_mut().cast(),
                iov_len: a.len(),
            },
            libc::iovec {
                iov_base: b.as_ptr().cast_mut().cast(),
                iov_len: b.len(),
            },
        ];
        // SAFETY: iov + a/b outlive the awaited op.
        let n = unsafe { ops::writev_at_raw(fd, iov.as_ptr(), 2, 0, 0) }
            .unwrap()
            .await
            .unwrap();
        assert_eq!(n, 4096 + 2048);

        let mut r0 = vec![0u8; 4096];
        let mut r1 = vec![0u8; 2048];
        let riov = [
            libc::iovec {
                iov_base: r0.as_mut_ptr().cast(),
                iov_len: r0.len(),
            },
            libc::iovec {
                iov_base: r1.as_mut_ptr().cast(),
                iov_len: r1.len(),
            },
        ];
        // SAFETY: riov + r0/r1 outlive the awaited op.
        let n = unsafe { ops::readv_at_raw(fd, riov.as_ptr(), 2, 0, 0) }
            .unwrap()
            .await
            .unwrap();
        assert_eq!(n, 4096 + 2048);
        assert!(r0.iter().all(|&x| x == 0x11));
        assert!(r1.iter().all(|&x| x == 0x22));
    });
}

#[test]
fn writev_readv_fixed_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixeddata");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let fd = file.as_raw_fd();

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    let reactor = rt.reactor().clone();
    rt.block_on(async move {
        // One registered buffer holds both the source and the read-back
        // region; the fixed ops reference it by index with iovecs that are
        // sub-ranges of it.
        let mut buf = vec![0u8; 16384];
        let Some(idx) = reactor.register_buffer(buf.as_ptr(), buf.len()) else {
            eprintln!("kernel lacks READV_FIXED/WRITEV_FIXED; skipping");
            return;
        };

        // Source: [0x11 ×4096][0x22 ×2048] at the front of the registered buf.
        buf[..4096].fill(0x11);
        buf[4096..6144].fill(0x22);
        let base = buf.as_ptr();
        // SAFETY: all offsets stay within the 16 KiB registered buffer.
        let (w1, r0, r1) = unsafe { (base.add(4096), base.add(8192), base.add(8192 + 4096)) };
        let wiov = [
            libc::iovec {
                iov_base: base.cast_mut().cast(),
                iov_len: 4096,
            },
            libc::iovec {
                iov_base: w1.cast_mut().cast(),
                iov_len: 2048,
            },
        ];
        // SAFETY: iov bases lie within registered buffer `idx`; buf outlives
        // the awaited op.
        let n = unsafe { ops::writev_fixed_at_raw(fd, wiov.as_ptr(), 2, 0, idx, 0) }
            .unwrap()
            .await
            .unwrap();
        assert_eq!(n, 6144);

        // Read back into the second half of the same registered buffer.
        let riov = [
            libc::iovec {
                iov_base: r0.cast_mut().cast(),
                iov_len: 4096,
            },
            libc::iovec {
                iov_base: r1.cast_mut().cast(),
                iov_len: 2048,
            },
        ];
        // SAFETY: as the write; the read-back region is within buffer `idx`.
        let n = unsafe { ops::readv_fixed_at_raw(fd, riov.as_ptr(), 2, 0, idx, 0) }
            .unwrap()
            .await
            .unwrap();
        assert_eq!(n, 6144);
        assert!(buf[8192..12288].iter().all(|&x| x == 0x11), "first segment");
        assert!(
            buf[12288..14336].iter().all(|&x| x == 0x22),
            "second segment"
        );

        reactor.unregister_buffer(idx);
    });
}

// The fixed-buffer table is a free-list of indices: distinct on the way out,
// exhausts when full, and reuses a released index. (No file IO — pure reactor
// accounting; skips where the kernel lacks fixed buffers.)
#[test]
fn fixed_buffer_table_accounting() {
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    let reactor = rt.reactor().clone();
    if !reactor.fixed_buffers_supported() {
        eprintln!("kernel lacks fixed buffers; skipping");
        return;
    }
    let buf = vec![0u8; 4096];

    // Drain the whole table; every slot hands out a distinct index.
    let mut idxs = Vec::new();
    while let Some(i) = reactor.register_buffer(buf.as_ptr(), buf.len()) {
        idxs.push(i);
    }
    assert!(!idxs.is_empty(), "supported but no slots handed out");
    let mut uniq = idxs.clone();
    uniq.sort_unstable();
    uniq.dedup();
    assert_eq!(uniq.len(), idxs.len(), "duplicate indices handed out");

    // Full table declines further registration (the connection then falls
    // back to plain readv/writev).
    assert!(
        reactor.register_buffer(buf.as_ptr(), buf.len()).is_none(),
        "expected None when the table is full"
    );

    // Releasing one slot frees exactly it for reuse.
    let freed = idxs.pop().unwrap();
    reactor.unregister_buffer(freed);
    assert_eq!(
        reactor.register_buffer(buf.as_ptr(), buf.len()),
        Some(freed),
        "released index should be the one reused"
    );

    for i in idxs {
        reactor.unregister_buffer(i);
    }
    reactor.unregister_buffer(freed);
}

#[test]
fn fallocate_punch_hole_zeroes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("holes");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let fd = file.as_raw_fd();

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async move {
        let data = vec![0xAAu8; 8192].into_boxed_slice();
        let (res, _) = ops::write_at(fd, data, 0).unwrap().await;
        assert_eq!(res.unwrap(), 8192);

        let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
        ops::fallocate(fd, mode, 0, 4096).unwrap().await.unwrap();

        let buf = vec![0xFFu8; 8192].into_boxed_slice();
        let (res, buf) = ops::read_at(fd, buf, 0).unwrap().await;
        assert_eq!(res.unwrap(), 8192);
        assert!(buf[..4096].iter().all(|&b| b == 0), "hole not zeroed");
        assert!(buf[4096..].iter().all(|&b| b == 0xAA), "data clobbered");
    });
}
