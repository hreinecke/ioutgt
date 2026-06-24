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
