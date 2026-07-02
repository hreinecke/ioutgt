//! IORING_OP_POLL_ADD futures: waking a queue thread's reactor on an arbitrary
//! external fd (the mechanism the RDMA transport uses to drain its
//! completion-channel fd without busy-polling).

use std::os::fd::RawFd;
use std::time::Duration;

use ioutgt_uring::{QueueRuntime, RingConfig, ops};

fn pollin() -> u32 {
    u32::try_from(libc::POLLIN).expect("POLLIN fits u32")
}

fn pipe() -> (RawFd, RawFd) {
    let mut fds = [0 as RawFd; 2];
    // SAFETY: pipe(2) fills the two-element array with valid fds on success.
    let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
    assert_eq!(rc, 0, "pipe failed: {}", std::io::Error::last_os_error());
    (fds[0], fds[1])
}

fn write_byte(fd: RawFd) {
    let b = [1u8];
    // SAFETY: fd is a valid pipe write end; writing one byte from a valid buffer.
    let n = unsafe { libc::write(fd, b.as_ptr().cast(), 1) };
    assert_eq!(n, 1, "write failed: {}", std::io::Error::last_os_error());
}

fn close(fd: RawFd) {
    // SAFETY: fd is a valid, owned fd closed exactly once.
    unsafe { libc::close(fd) };
}

#[test]
fn poll_add_ready_fd_completes_with_pollin() {
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async {
        let (rd, wr) = pipe();
        write_byte(wr); // read end is now readable
        let events = ops::poll_add(rd, pollin()).unwrap().await.unwrap();
        assert!(events & pollin() != 0, "expected POLLIN, got {events:#x}");
        close(rd);
        close(wr);
    });
}

#[test]
fn poll_add_wakes_when_fd_becomes_ready() {
    let rt = QueueRuntime::new(RingConfig::default()).unwrap();
    rt.block_on(async {
        let (rd, wr) = pipe();
        let waiter =
            tokio::task::spawn_local(async move { ops::poll_add(rd, pollin()).unwrap().await });
        // Let the poll park in the reactor (submit_and_wait), then make the fd
        // readable — proving the reactor wakes on the fd, not on a busy spin.
        ops::sleep(Duration::from_millis(20))
            .unwrap()
            .await
            .unwrap();
        write_byte(wr);
        let events = waiter.await.unwrap().unwrap();
        assert!(events & pollin() != 0, "expected POLLIN, got {events:#x}");
        close(rd);
        close(wr);
    });
}
