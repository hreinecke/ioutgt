//! SPIKE (uncommitted): prove io_uring zero-copy receive (zcrx) in NODEV mode
//! with NO NIC, over a loopback TCP socket, using the `io-uring` crate (0.7.12)
//! plus raw constants where the crate falls short.
//!
//! Goal: register a `ZCRX_REG_NODEV` ifq against a private CQE32 ring, issue one
//! `RecvZc` over a 127.0.0.1 TCP pair, read the 32-byte completion's embedded
//! `io_uring_zcrx_cqe.off`, and verify the bytes the kernel copied into our app
//! area match what was sent.
//!
//! This is NOT wired into the reactor and touches no production code. It is a
//! de-risking experiment; see `docs/zcrx-spike-notes.md` for the findings.
//!
//! Runtime requirements (else the test SKIPs, printing the reason):
//!   * kernel with io_uring zcrx + ZCRX_REG_NODEV (>= the bleeding-edge tree),
//!   * CAP_NET_ADMIN — `io_register_zcrx()` gates on it *before* the NODEV
//!     branch, so a normal user gets EPERM. Run as root to exercise the data
//!     path:  `cargo test -p ioutgt-uring --test zcrx_nodev -- --nocapture`
//!     under `sudo -E`, or run the compiled test binary directly under sudo.

use std::io;
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;

use io_uring::{IoUring, cqueue, opcode, squeue, types};

// --- Constants the crate does NOT expose (raw from uapi/linux/io_uring/zcrx.h
//     and uapi/linux/io_uring.h). Documenting each gap is a spike deliverable.
const ZCRX_REG_NODEV: u32 = 2; // enum zcrx_reg_flags
const AREA_SHIFT: u64 = 48; // IORING_ZCRX_AREA_SHIFT (crate has it as types::IORING_ZCRX_AREA_SHIFT, kept local for clarity)
const AREA_MASK: u64 = (1u64 << AREA_SHIFT) - 1;
// IORING_MEM_REGION_TYPE_USER *is* re-exported as types::IORING_MEM_REGION_TYPE_USER.

const AREA_LEN: usize = 1 << 20; // 1 MiB app area
const REGION_LEN: usize = 4096; // refill-ring region: one page is plenty for 64 rqes
const RQ_ENTRIES: u32 = 64;
const PAYLOAD: &[u8] = b"zcrx-nodev-spike: the quick brown fox copies bytes without a NIC";

/// page-aligned anonymous RW mapping; returns base pointer.
fn map_anon(len: usize) -> *mut u8 {
    // SAFETY: standard anonymous private mapping; len > 0, fd = -1.
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    assert!(
        p != libc::MAP_FAILED,
        "mmap failed: {}",
        io::Error::last_os_error()
    );
    p.cast()
}

fn unmap(p: *mut u8, len: usize) {
    // SAFETY: p/len come from a matching successful map_anon().
    unsafe {
        libc::munmap(p.cast(), len);
    }
}

#[test]
fn zcrx_nodev_loopback_copy() {
    // 1. Private CQE32 ring. RecvZc/zcrx mandates DEFER_TASKRUN (=> SINGLE_ISSUER).
    //    Entry32::BUILD_FLAGS already ORs IORING_SETUP_CQE32 in for us.
    let ring = IoUring::<squeue::Entry, cqueue::Entry32>::builder()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .setup_clamp()
        .build(RQ_ENTRIES);
    let mut ring = match ring {
        Ok(r) => r,
        Err(e) => {
            eprintln!("SKIP: cannot build CQE32/DEFER_TASKRUN ring: {e}");
            return;
        }
    };

    // 2. App area (kernel copies received bytes here) + 3. refill-ring region.
    let area = map_anon(AREA_LEN);
    let region = map_anon(REGION_LEN);

    // 4. Build the registration structs.
    //
    //    Crate gap: `io_uring_zcrx_offsets` is NOT re-exported from `io_uring::types`
    //    (only ifq_reg/area_reg/cqe/rqe/region_desc are), so we cannot name it to
    //    write a struct literal for `ifq_reg.offsets`. Worked around via the
    //    derived `Default` (`..Default::default()`), which zero-fills it; the
    //    kernel writes head/tail/rqes back on success.
    //
    //    Crate gap: the 0.7.12 bindgen of `io_uring_zcrx_ifq_reg` predates the
    //    `rx_buf_len`/`notif_desc` fields — it labels them `__resv2: u32` and
    //    folds `notif_desc` into `__resv: [u64;3]`. Total size is identical
    //    (96 bytes), and we want rx_buf_len=0 (NODEV requires PAGE_SHIFT) and
    //    notif_desc=0, so leaving them zeroed is exactly correct.
    let area_reg = types::io_uring_zcrx_area_reg {
        addr: area as u64,
        len: AREA_LEN as u64,
        flags: 0,
        ..Default::default()
    };
    // Refill ring is app-provided memory (TYPE_USER => kernel pins our pages).
    let region_desc = types::io_uring_region_desc {
        user_addr: region as u64,
        size: REGION_LEN as u64,
        flags: types::IORING_MEM_REGION_TYPE_USER,
        ..Default::default()
    };
    let reg = types::io_uring_zcrx_ifq_reg {
        if_rxq: 0,
        rq_entries: RQ_ENTRIES,
        flags: ZCRX_REG_NODEV,
        area_ptr: std::ptr::addr_of!(area_reg) as u64,
        region_ptr: std::ptr::addr_of!(region_desc) as u64,
        ..Default::default()
    };

    // 5. Register. Skip (not fail) on the expected "environment can't run this"
    //    errnos, recording which one fired.
    if let Err(e) = ring.submitter().register_ifq(&reg) {
        let raw = e.raw_os_error().unwrap_or(0);
        let why = match raw {
            libc::EPERM => "EPERM (need CAP_NET_ADMIN — run as root)",
            libc::ENOSYS => "ENOSYS (kernel lacks IORING_REGISTER_ZCRX_IFQ)",
            libc::EOPNOTSUPP => "EOPNOTSUPP (zcrx/NODEV not supported)",
            libc::EINVAL => "EINVAL (struct/flags rejected — possible ABI mismatch)",
            _ => "unexpected errno",
        };
        eprintln!("SKIP: register_ifq(ZCRX_REG_NODEV) -> {why}: {e}");
        unmap(area, AREA_LEN);
        unmap(region, REGION_LEN);
        return;
    }

    let zcrx_id = reg.zcrx_id;
    // Document what the kernel wrote back. `reg.__resv2` is really `rx_buf_len`.
    eprintln!(
        "registered zcrx ifq: id={zcrx_id} rx_buf_len(crate __resv2)={} offsets.head={} tail={} rqes={}",
        reg.__resv2, reg.offsets.head, reg.offsets.tail, reg.offsets.rqes
    );

    // 6. Loopback TCP pair. zcrx is TCP-only (io_zcrx_recv checks
    //    prot->recvmsg == tcp_recvmsg) so AF_UNIX is out.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).unwrap();
    let (server, _peer) = listener.accept().unwrap();
    client.set_nodelay(true).unwrap();

    // Send known bytes client -> server BEFORE issuing RecvZc.
    use std::io::Write;
    (&client).write_all(PAYLOAD).unwrap();
    (&client).flush().unwrap();

    // 7. Issue RecvZc on the server fd.
    //    Crate quirk: RecvZc::build() unconditionally ORs IORING_RECV_MULTISHOT
    //    into ioprio — there is no single-shot RecvZc via the safe builder. With
    //    a bounded len it still terminates once `len` bytes are consumed.
    let sqe = opcode::RecvZc::new(types::Fd(server.as_raw_fd()), PAYLOAD.len() as u32)
        .ifq(zcrx_id)
        .build()
        .user_data(0x5043_5243); // 'ZCRX'-ish

    // SAFETY: the fd outlives the in-flight op (we hold `server`); no buffers
    // referenced by the SQE other than the fd.
    unsafe {
        ring.submission().push(&sqe).expect("SQ full");
    }

    // 8. Pump until we see the data CQE (result>0) or a terminal error.
    let mut data_cqe: Option<(i32, [u64; 2])> = None;
    let mut err_cqe: Option<i32> = None;
    'pump: for _ in 0..16 {
        ring.submit_and_wait(1).expect("submit_and_wait");
        let cqes: Vec<cqueue::Entry32> = ring.completion().collect();
        for c in cqes {
            let res = c.result();
            eprintln!(
                "CQE32 user_data={:#x} res={res} flags={:#x} big_cqe=[{:#x},{:#x}]",
                c.user_data(),
                c.flags(),
                c.big_cqe()[0],
                c.big_cqe()[1]
            );
            if res > 0 {
                data_cqe = Some((res, *c.big_cqe()));
                break 'pump;
            } else if res < 0 {
                err_cqe = Some(res);
                break 'pump;
            }
            // res == 0 with F_MORE cleared would be the multishot terminator.
        }
    }

    if let Some(res) = err_cqe {
        // -ENOMEM here would mean the area freelist gave no buffer, etc.
        unmap(area, AREA_LEN);
        unmap(region, REGION_LEN);
        panic!(
            "RecvZc completed with error {res} ({})",
            io::Error::from_raw_os_error(-res)
        );
    }

    let (len, big) = data_cqe.expect("no data CQE observed within pump budget");
    let len = len as usize;

    // 9. Decode io_uring_zcrx_cqe.off (big_cqe[0]); big_cqe[1] is __pad (0).
    let off = big[0];
    let area_id = off >> AREA_SHIFT;
    let offset = (off & AREA_MASK) as usize;
    assert_eq!(area_id, 0, "single area => area_id 0");
    assert_eq!(big[1], 0, "zcrx_cqe.__pad must be 0");
    assert!(offset + len <= AREA_LEN, "decoded slice escapes area");

    // 10. The kernel copied `len` bytes to area+offset.
    // SAFETY: offset+len bounds-checked above; area is a live AREA_LEN mapping.
    let got = unsafe { std::slice::from_raw_parts(area.add(offset), len) };
    assert_eq!(len, PAYLOAD.len(), "copied length mismatch");
    assert_eq!(got, PAYLOAD, "copied bytes mismatch");

    eprintln!("OK: zcrx NODEV copied {len} bytes to area+{offset}, contents verified");

    unmap(area, AREA_LEN);
    unmap(region, REGION_LEN);
}
