//! Runtime feature probing for the running kernel.

use std::io;

use io_uring::{IoUring, Probe, opcode};

/// io_uring capabilities of the running kernel that ioutgt cares about.
#[derive(Debug, Clone, Copy)]
#[allow(missing_docs)]
pub struct Features {
    /// `SINGLE_ISSUER | DEFER_TASKRUN` ring creation works (6.1+).
    pub defer_taskrun: bool,
    pub recv: bool,
    pub send: bool,
    pub sendmsg: bool,
    pub send_zc: bool,
    pub accept: bool,
    pub accept_multi: bool,
    pub read: bool,
    pub write: bool,
    pub fsync: bool,
    pub fallocate: bool,
    pub timeout: bool,
    pub async_cancel: bool,
    /// Provided-buffer-ring registration is available (phase-2 multishot
    /// recv).
    pub buf_ring: bool,
}

impl Features {
    /// Everything the phase-1 data path requires.
    pub fn phase1_ok(&self) -> bool {
        self.defer_taskrun
            && self.recv
            && self.send
            && self.sendmsg
            && self.accept_multi
            && self.read
            && self.write
            && self.fsync
            && self.fallocate
            && self.timeout
            && self.async_cancel
    }
}

/// Probe the running kernel.
pub fn probe() -> io::Result<Features> {
    let defer_taskrun = IoUring::<io_uring::squeue::Entry>::builder()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .build(8)
        .is_ok();

    let ring = IoUring::new(8)?;
    let mut probe = Probe::new();
    ring.submitter().register_probe(&mut probe)?;

    let buf_ring = probe.is_supported(opcode::ProvideBuffers::CODE);

    Ok(Features {
        defer_taskrun,
        recv: probe.is_supported(opcode::Recv::CODE),
        send: probe.is_supported(opcode::Send::CODE),
        sendmsg: probe.is_supported(opcode::SendMsg::CODE),
        send_zc: probe.is_supported(opcode::SendZc::CODE),
        accept: probe.is_supported(opcode::Accept::CODE),
        accept_multi: probe.is_supported(opcode::AcceptMulti::CODE),
        read: probe.is_supported(opcode::Read::CODE),
        write: probe.is_supported(opcode::Write::CODE),
        fsync: probe.is_supported(opcode::Fsync::CODE),
        fallocate: probe.is_supported(opcode::Fallocate::CODE),
        timeout: probe.is_supported(opcode::Timeout::CODE),
        async_cancel: probe.is_supported(opcode::AsyncCancel::CODE),
        buf_ring,
    })
}
