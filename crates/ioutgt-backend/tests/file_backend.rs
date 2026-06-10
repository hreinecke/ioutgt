//! FileBackend unit tests: O_DIRECT roundtrip, write-zeroes, discard.
//!
//! Backed by a file under `target/` (a real filesystem — /tmp is tmpfs
//! here and refuses O_DIRECT, which would exercise only the buffered
//! fallback).

use ioutgt_backend::FileBackend;
use ioutgt_core::buf::AlignedBuf;
use ioutgt_core::{Backend, BackendError, LbaRange};
use ioutgt_uring::{QueueRuntime, RingConfig};

fn scratch_file(name: &str, size: u64) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"));
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join(name);
    let file = std::fs::File::create(&path).unwrap();
    file.set_len(size).unwrap();
    path
}

#[test]
fn direct_write_read_roundtrip() {
    let path = scratch_file("fb-roundtrip", 8 << 20);
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    let be = FileBackend::open(&path, 9).unwrap();
    eprintln!("O_DIRECT active: {}", be.is_direct());
    assert_eq!(be.nr_blocks(), (8 << 20) / 512);

    rt.block_on(async move {
        let mut buf = AlignedBuf::zeroed(128 * 1024);
        #[allow(clippy::cast_possible_truncation)]
        buf.iter_mut()
            .enumerate()
            .for_each(|(i, b)| *b = (i % 251) as u8);
        let pattern = buf.to_vec();

        be.write(64, &buf[..128 * 1024]).await.unwrap();
        be.flush().await.unwrap();

        let mut out = AlignedBuf::zeroed(128 * 1024);
        be.read(64, &mut out[..128 * 1024]).await.unwrap();
        assert_eq!(&out[..], &pattern[..], "128K roundtrip");

        // Write-zeroes the first 8K of the range and re-check.
        be.write_zeroes(LbaRange { slba: 64, nlb: 16 })
            .await
            .unwrap();
        be.read(64, &mut out[..128 * 1024]).await.unwrap();
        assert!(out[..8192].iter().all(|&b| b == 0), "zeroed range");
        assert_eq!(&out[8192..], &pattern[8192..], "rest untouched");

        // Discard is a hint: must succeed; reads stay readable.
        be.discard(&[LbaRange { slba: 64, nlb: 256 }])
            .await
            .unwrap();
        be.read(64, &mut out[..4096]).await.unwrap();

        // Out-of-range rejected.
        let err = be.read(be.nr_blocks(), &mut out[..512]).await.unwrap_err();
        assert_eq!(err, BackendError::OutOfRange);
    });
}

#[test]
fn open_rejects_missing_and_tiny() {
    assert!(FileBackend::open(std::path::Path::new("/nonexistent/x"), 9).is_err());
    let path = scratch_file("fb-tiny", 256); // < one block
    assert!(FileBackend::open(&path, 9).is_err());
}
