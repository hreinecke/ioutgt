//! Cross-thread mailbox: the only sanctioned way into a queue thread.
//!
//! Senders (control thread) push onto a mutex-protected queue — control
//! plane rate, so a mutex is fine — and ring an eventfd doorbell. The
//! receiving queue thread keeps an async read armed on the eventfd through
//! its ring, so a doorbell CQE wakes it even while parked in
//! `io_uring_enter`.

use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::{Arc, Mutex};

struct Shared<T> {
    queue: Mutex<VecDeque<T>>,
    doorbell: OwnedFd,
}

/// Sending half; clonable, usable from any thread.
pub struct MailboxSender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Clone for MailboxSender<T> {
    fn clone(&self) -> Self {
        MailboxSender {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T: Send> MailboxSender<T> {
    /// Push a message and ring the doorbell.
    pub fn send(&self, value: T) {
        self.shared
            .queue
            .lock()
            .expect("mailbox poisoned")
            .push_back(value);
        let one: u64 = 1;
        // SAFETY: valid eventfd, valid 8-byte source. An eventfd write of
        // 1 cannot fail short of counter overflow (practically
        // unreachable); the result is intentionally ignored.
        unsafe {
            libc::write(self.shared.doorbell.as_raw_fd(), (&raw const one).cast(), 8);
        }
    }
}

/// Receiving half; lives on the queue thread.
pub struct Mailbox<T> {
    shared: Arc<Shared<T>>,
    read_buf: Option<Box<[u8]>>,
}

impl<T> Mailbox<T> {
    /// Non-blocking pop.
    pub fn try_recv(&self) -> Option<T> {
        self.shared
            .queue
            .lock()
            .expect("mailbox poisoned")
            .pop_front()
    }

    /// Await the next message, sleeping on the ring while idle.
    pub async fn recv(&mut self) -> io::Result<T> {
        loop {
            if let Some(value) = self.try_recv() {
                return Ok(value);
            }
            // Arm the doorbell read *after* the empty check: a send racing
            // with us leaves the eventfd counter non-zero, so the read
            // completes immediately and we re-check.
            let buf = self.read_buf.take().expect("mailbox read buffer leaked");
            let (result, buf) =
                crate::ops::read_at(self.shared.doorbell.as_raw_fd(), buf, 0)?.await;
            self.read_buf = Some(buf);
            result?;
        }
    }
}

/// Create a connected sender/receiver pair.
pub fn mailbox<T: Send>() -> io::Result<(MailboxSender<T>, Mailbox<T>)> {
    // SAFETY: plain eventfd creation; the fd is immediately wrapped in
    // OwnedFd on the success path.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fresh fd, exclusively owned.
    let doorbell = unsafe { OwnedFd::from_raw_fd(fd) };
    let shared = Arc::new(Shared {
        queue: Mutex::new(VecDeque::new()),
        doorbell,
    });
    Ok((
        MailboxSender {
            shared: Arc::clone(&shared),
        },
        Mailbox {
            shared,
            read_buf: Some(vec![0u8; 8].into_boxed_slice()),
        },
    ))
}
