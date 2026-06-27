//! FileBackend unit tests: O_DIRECT roundtrip, write-zeroes, discard.
//!
//! Backed by a file under `target/` (a real filesystem — /tmp is tmpfs
//! here and refuses O_DIRECT, which would exercise only the buffered
//! fallback).

use ioutgt_backend::FileBackend;
use ioutgt_core::buf::AlignedBuf;
use ioutgt_core::pool::Seg;
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
    let be = FileBackend::open(&path, 9, false).unwrap();
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
fn scattered_write_matches_contiguous() {
    // A two-segment vectored write must land byte-identically to one
    // contiguous write of the concatenation.
    let path = scratch_file("fb-scatter", 8 << 20);
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    let be = FileBackend::open(&path, 12, false).unwrap(); // 4K blocks

    rt.block_on(async move {
        // Two separate page-aligned buffers (non-adjacent in memory).
        let mut a = AlignedBuf::zeroed(8192);
        let mut b = AlignedBuf::zeroed(4096);
        a.iter_mut().for_each(|x| *x = 0xAB);
        b.iter_mut().for_each(|x| *x = 0xCD);
        let segs = [
            Seg {
                ptr: a.as_ptr().cast_mut(),
                len: 8192,
            },
            Seg {
                ptr: b.as_ptr().cast_mut(),
                len: 4096,
            },
        ];
        be.write_segs(0, &segs, 12288, None).await.unwrap();
        be.flush().await.unwrap();

        // Read it back vectored into fresh buffers and check the seam.
        let r = AlignedBuf::zeroed(12288);
        let rsegs = [Seg {
            ptr: r.as_ptr().cast_mut(),
            len: 12288,
        }];
        be.read_segs(0, &rsegs, 12288, None).await.unwrap();
        assert!(r[..8192].iter().all(|&x| x == 0xAB), "first segment");
        assert!(r[8192..].iter().all(|&x| x == 0xCD), "second segment");
    });
}

#[test]
fn temp_dir_roundtrip_either_path() {
    // temp_dir may be tmpfs (no O_DIRECT → buffered/DONTCACHE path) or a
    // real fs (O_DIRECT path); the backend must open and round-trip on
    // whichever it is.
    let dir = std::env::temp_dir().join(format!("ioutgt-fb-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("img");
    std::fs::File::create(&path)
        .unwrap()
        .set_len(1 << 20)
        .unwrap();

    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    let be = FileBackend::open(&path, 9, false).unwrap();
    eprintln!("temp_dir O_DIRECT active: {}", be.is_direct());
    rt.block_on(async move {
        let mut buf = AlignedBuf::zeroed(4096);
        buf.iter_mut().for_each(|x| *x = 0x5A);
        let pattern = buf.to_vec();
        be.write(8, &buf[..4096]).await.unwrap();
        let mut out = AlignedBuf::zeroed(4096);
        be.read(8, &mut out[..4096]).await.unwrap();
        assert_eq!(&out[..], &pattern[..]);
    });
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ring_off_keeps_direct_on_real_fs() {
    // The scratch file lives under target/ (a real filesystem per this
    // module's docs), which supports O_DIRECT. With the recv ring OFF (the
    // default), O_DIRECT must be kept even though such a store typically
    // reports a `dio_mem` of 512 (> 4) — the gate the ring needs must not
    // pessimize the default page-aligned-buffer path into buffered IO.
    let path = scratch_file("fb-ring-off-direct", 8 << 20);
    let be_off = FileBackend::open(&path, 9, false).unwrap();
    assert!(
        be_off.is_direct(),
        "ring off on a real fs must keep O_DIRECT"
    );

    // Ring on is never *more* permissive than ring off: if it kept O_DIRECT,
    // ring off must have too.
    let be_on = FileBackend::open(&path, 9, true).unwrap();
    assert!(
        be_off.is_direct() || !be_on.is_direct(),
        "ring off must be at least as direct as ring on"
    );
}

#[test]
fn open_rejects_missing_and_tiny() {
    assert!(FileBackend::open(std::path::Path::new("/nonexistent/x"), 9, false).is_err());
    let path = scratch_file("fb-tiny", 256); // < one block
    assert!(FileBackend::open(&path, 9, false).is_err());
}
