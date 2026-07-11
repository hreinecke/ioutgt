//! Thread-level OS introspection: kernel tid and live CPU affinity,
//! reported as kernel cpulists for control-plane visibility.

/// Kernel thread id of the calling thread.
pub fn current_tid() -> i32 {
    // SAFETY: gettid has no preconditions and cannot fail.
    unsafe { libc::gettid() }
}

/// Current thread's CPU affinity (see [`cpus_of`]).
pub fn current_cpus() -> String {
    cpus_of(0)
}

/// CPU affinity of thread `tid` (0 = calling thread) as a kernel cpulist
/// ("3", "0-3,8"), "*" when the mask covers every online CPU, "?" if the
/// query fails. Reads the *live* affinity, so it reflects any re-pinning
/// done after the thread started.
pub fn cpus_of(tid: i32) -> String {
    // SAFETY: a zeroed cpu_set_t is a valid value for the call to
    // overwrite; sched_getaffinity writes within size_of::<cpu_set_t>().
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: `tid` names a thread in this process (0 = calling thread);
    // the buffer is a real cpu_set_t and the size passed matches it.
    let rc =
        unsafe { libc::sched_getaffinity(tid, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
    if rc != 0 {
        return "?".to_owned();
    }
    // SAFETY: `set` was initialized by sched_getaffinity above.
    let count = unsafe { libc::CPU_COUNT(&set) };
    // SAFETY: sysconf has no preconditions.
    let online = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
    if online > 0 && i64::from(count) >= online {
        return "*".to_owned();
    }
    fn flush(out: &mut String, run: (usize, usize)) {
        use std::fmt::Write;
        if !out.is_empty() {
            out.push(',');
        }
        let _ = if run.0 == run.1 {
            write!(out, "{}", run.0)
        } else {
            write!(out, "{}-{}", run.0, run.1)
        };
    }
    let mut out = String::new();
    let mut run: Option<(usize, usize)> = None;
    #[allow(clippy::cast_sign_loss)] // CPU_SETSIZE is a positive constant
    for cpu in 0..(libc::CPU_SETSIZE as usize) {
        // SAFETY: cpu < CPU_SETSIZE bounds the bit lookup.
        if unsafe { libc::CPU_ISSET(cpu, &set) } {
            run = match run {
                Some((start, end)) if end + 1 == cpu => Some((start, cpu)),
                Some(prev) => {
                    flush(&mut out, prev);
                    Some((cpu, cpu))
                }
                None => Some((cpu, cpu)),
            };
        }
    }
    if let Some(prev) = run {
        flush(&mut out, prev);
    }
    out
}
