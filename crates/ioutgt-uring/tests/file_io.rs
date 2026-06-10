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
