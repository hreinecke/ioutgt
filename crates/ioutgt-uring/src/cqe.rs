use std::io;

/// Result of one completed io_uring operation (one CQE).
#[derive(Debug, Clone, Copy)]
pub struct CqeResult {
    /// Raw CQE `res` field: ≥ 0 on success, `-errno` on failure.
    pub result: i32,
    /// Raw CQE flags (`IORING_CQE_F_*`).
    pub flags: u32,
}

impl CqeResult {
    /// Convert the raw result into an `io::Result`, mapping negative
    /// values to their errno.
    pub fn io(self) -> io::Result<u32> {
        if self.result < 0 {
            Err(io::Error::from_raw_os_error(-self.result))
        } else {
            #[allow(clippy::cast_sign_loss)]
            Ok(self.result as u32)
        }
    }

    /// Whether the kernel will post further CQEs for this op
    /// (`IORING_CQE_F_MORE`, multishot).
    pub fn more(self) -> bool {
        io_uring::cqueue::more(self.flags)
    }
}
