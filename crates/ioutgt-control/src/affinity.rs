//! NIC RX/TX queue IRQ ↔ io-thread CPU affinity sync (`SET_AFFINITY`).
//!
//! Direction is chosen per NIC queue by whether its IRQ affinity is
//! kernel-managed (writes to `smp_affinity` rejected):
//!
//! - **unmanaged** → write the io-thread's assigned CPU to the NIC queue
//!   IRQ's `smp_affinity` (NIC follows ioutgt).
//! - **managed** → pull the NIC IRQ's effective affinity into the io-thread
//!   CPU assignment (ioutgt follows NIC). The pool is spawned lazily on the
//!   first connection, so a mutation made before connect pins the io-thread
//!   to the NIC's CPU when it spawns — no live re-pin needed (run the op
//!   before `connect`, which is also what keeps slot buffers NUMA-local).
//!
//! Queue index `i` maps to io-thread index `i`. The `/proc/interrupts`
//! parsing is pure and unit-tested; the rest is thin sysfs/procfs IO.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde_json::{Value, json};

/// rx/tx IRQ number for each NIC queue index (combined channels put the
/// same IRQ in both maps).
#[derive(Debug, Default, PartialEq, Eq)]
pub struct NicIrqs {
    /// queue index → rx IRQ number.
    pub rx: BTreeMap<usize, u32>,
    /// queue index → tx IRQ number.
    pub tx: BTreeMap<usize, u32>,
}

impl NicIrqs {
    /// Highest queue index seen + 1 (0 when empty).
    fn nr_queues(&self) -> usize {
        self.rx
            .keys()
            .chain(self.tx.keys())
            .copied()
            .max()
            .map_or(0, |m| m + 1)
    }
}

enum Role {
    TxRx,
    Rx,
    Tx,
}

/// Match an IRQ action token like `<nic>-TxRx-3`, `<nic>-rx-3`, `<nic>-tx-3`
/// (a driver prefix such as `bnxt_en-<nic>-TxRx-3` still matches, since we
/// search for the NIC substring). Returns the role and queue index.
fn match_label(tok: &str, nic: &str) -> Option<(Role, usize)> {
    let pos = tok.find(nic)?;
    let after = tok[pos + nic.len()..].strip_prefix('-')?;
    let (role, num) = if let Some(n) = after.strip_prefix("TxRx-") {
        (Role::TxRx, n)
    } else if let Some(n) = after.strip_prefix("rx-") {
        (Role::Rx, n)
    } else if let Some(n) = after.strip_prefix("tx-") {
        (Role::Tx, n)
    } else {
        return None;
    };
    // `num` must be exactly the trailing index (no further `-suffix`).
    Some((role, num.parse().ok()?))
}

/// Parse `/proc/interrupts` text into per-queue rx/tx IRQ numbers for `nic`.
pub fn parse_interrupts(text: &str, nic: &str) -> NicIrqs {
    let mut irqs = NicIrqs::default();
    for line in text.lines() {
        let Some((irq_str, rest)) = line.split_once(':') else {
            continue;
        };
        let Ok(irq) = irq_str.trim().parse::<u32>() else {
            continue;
        };
        for tok in rest.split_whitespace() {
            match match_label(tok, nic) {
                Some((Role::TxRx, idx)) => {
                    irqs.rx.insert(idx, irq);
                    irqs.tx.insert(idx, irq);
                }
                Some((Role::Rx, idx)) => {
                    irqs.rx.insert(idx, irq);
                }
                Some((Role::Tx, idx)) => {
                    irqs.tx.insert(idx, irq);
                }
                None => {}
            }
        }
    }
    irqs
}

/// First CPU id in a kernel cpulist (`"12"`, `"12,44"`, `"0-3"`).
fn first_cpu(list: &str) -> Option<usize> {
    list.split([',', '-'])
        .next()
        .and_then(|s| s.trim().parse().ok())
}

fn irq_eff(irq: u32) -> Option<String> {
    std::fs::read_to_string(format!("/proc/irq/{irq}/effective_affinity_list"))
        .ok()
        .map(|s| s.trim().to_owned())
}

/// Write a single CPU to an IRQ's `smp_affinity_list`. `Err` with
/// `ErrorKind`/raw errno lets the caller detect a managed IRQ (EIO).
fn write_irq_cpu(irq: u32, cpu: usize) -> std::io::Result<()> {
    std::fs::write(
        format!("/proc/irq/{irq}/smp_affinity_list"),
        cpu.to_string(),
    )
}

/// Distinct IRQs serving queue `idx` (rx then tx; deduped for combined).
fn distinct_irqs(irqs: &NicIrqs, idx: usize) -> Vec<u32> {
    let mut v = Vec::new();
    if let Some(&r) = irqs.rx.get(&idx) {
        v.push(r);
    }
    if let Some(&t) = irqs.tx.get(&idx) {
        if !v.contains(&t) {
            v.push(t);
        }
    }
    v
}

/// Sync NIC `nic`'s per-queue IRQs with the io-thread CPU assignment.
/// Mutates `io_cpus` for managed (or unpinned-io-thread) queues. Returns a
/// JSON report, or an error string when the NIC has no recognizable IRQs.
pub fn sync(nic: &str, io_cpus: &Mutex<Vec<Option<usize>>>) -> Result<Value, String> {
    let text = std::fs::read_to_string("/proc/interrupts")
        .map_err(|e| format!("read /proc/interrupts: {e}"))?;
    let irqs = parse_interrupts(&text, nic);
    if irqs.rx.is_empty() && irqs.tx.is_empty() {
        return Err(format!(
            "no rx/tx IRQs for nic '{nic}' in /proc/interrupts \
             (is the nic in this netns? unexpected action label?)"
        ));
    }
    let n_threads = io_cpus.lock().expect("io_cpus mutex").len();
    let n_queues = irqs.nr_queues();
    let mut rows = Vec::with_capacity(n_queues);
    for idx in 0..n_queues {
        rows.push(sync_queue(&irqs, idx, n_threads, io_cpus));
    }
    Ok(json!({
        "nic": nic,
        "io_threads": n_threads,
        "nic_queues": n_queues,
        "queues": rows,
    }))
}

fn sync_queue(
    irqs: &NicIrqs,
    idx: usize,
    n_threads: usize,
    io_cpus: &Mutex<Vec<Option<usize>>>,
) -> Value {
    let queue_irqs = distinct_irqs(irqs, idx);
    let rx_irq = irqs.rx.get(&idx).copied();
    let tx_irq = irqs.tx.get(&idx).copied();
    let eff_before: Vec<String> = queue_irqs.iter().filter_map(|&i| irq_eff(i)).collect();

    let base = |action: &str, managed: Option<bool>| {
        json!({
            "queue": idx,
            "rx_irq": rx_irq,
            "tx_irq": tx_irq,
            "managed": managed,
            "nic_eff_before": eff_before,
            "nic_eff_after": queue_irqs.iter().filter_map(|&i| irq_eff(i)).collect::<Vec<_>>(),
            "action": action,
        })
    };

    if idx >= n_threads {
        return base("skipped: more NIC queues than io-threads", None);
    }
    let assigned = io_cpus.lock().expect("io_cpus mutex")[idx];

    // The CPU to adopt if we pull NIC→io-thread: the rx IRQ's effective CPU
    // (recv is the hot path), else the tx IRQ's.
    let pull_cpu = || {
        rx_irq
            .or(tx_irq)
            .and_then(irq_eff)
            .as_deref()
            .and_then(first_cpu)
    };

    match assigned {
        Some(cpu) => {
            // Try the unmanaged direction: NIC follows the io-thread.
            let mut managed = false;
            let mut errno = String::new();
            for &irq in &queue_irqs {
                if let Err(e) = write_irq_cpu(irq, cpu) {
                    managed = true;
                    errno = e.to_string();
                    break;
                }
            }
            if !managed {
                base(
                    &format!(
                        "nic-follows-iothread: irq(s) {queue_irqs:?} smp_affinity -> cpu {cpu}"
                    ),
                    Some(false),
                )
            } else if let Some(pull) = pull_cpu() {
                io_cpus.lock().expect("io_cpus mutex")[idx] = Some(pull);
                base(
                    &format!(
                        "managed ({errno}): iothread-follows-nic: io-thread {idx} cpu {cpu} -> {pull} (applies on next connect)"
                    ),
                    Some(true),
                )
            } else {
                base(
                    &format!(
                        "managed ({errno}) but NIC effective affinity unreadable; left unchanged"
                    ),
                    Some(true),
                )
            }
        }
        None => {
            // io-thread unpinned: only the pull direction makes sense.
            if let Some(pull) = pull_cpu() {
                io_cpus.lock().expect("io_cpus mutex")[idx] = Some(pull);
                base(
                    &format!(
                        "iothread unpinned: iothread-follows-nic: io-thread {idx} cpu -> {pull} (applies on next connect)"
                    ),
                    None,
                )
            } else {
                base(
                    "iothread unpinned and NIC effective affinity unreadable; skipped",
                    None,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
            201:  0  0  IR-PCI-MSIX-0000:02:00.0  0-edge  enp2s0f0np0\n\
            202:  10 0  IR-PCI-MSIX-0000:02:00.0  1-edge  enp2s0f0np0-TxRx-0\n\
            203:  5  0  IR-PCI-MSIX-0000:02:00.0  2-edge  enp2s0f0np0-TxRx-1\n\
            999:  1  0  IR-PCI-MSIX  3-edge  someother-TxRx-0\n";

    #[test]
    fn parses_combined_txrx_queues() {
        let irqs = parse_interrupts(SAMPLE, "enp2s0f0np0");
        assert_eq!(irqs.rx.get(&0), Some(&202));
        assert_eq!(irqs.tx.get(&0), Some(&202)); // combined: same IRQ
        assert_eq!(irqs.rx.get(&1), Some(&203));
        assert_eq!(irqs.nr_queues(), 2);
        // The bare "enp2s0f0np0" line (no -TxRx-N) and the other NIC are ignored.
        assert!(!irqs.rx.contains_key(&2));
    }

    #[test]
    fn parses_split_rx_tx() {
        let text = "\
            10: 0 0 chip x-edge eth9-rx-0\n\
            11: 0 0 chip x-edge eth9-tx-0\n\
            12: 0 0 chip x-edge eth9-rx-1\n";
        let irqs = parse_interrupts(text, "eth9");
        assert_eq!(irqs.rx.get(&0), Some(&10));
        assert_eq!(irqs.tx.get(&0), Some(&11));
        assert_eq!(irqs.rx.get(&1), Some(&12));
        assert_eq!(irqs.tx.get(&1), None);
        assert_eq!(distinct_irqs(&irqs, 0), vec![10, 11]);
        assert_eq!(distinct_irqs(&irqs, 1), vec![12]);
    }

    #[test]
    fn no_false_prefix_match() {
        // "eth1" must not match "eth10-TxRx-0".
        let text = "5: 0 0 chip edge eth10-TxRx-0\n";
        assert_eq!(parse_interrupts(text, "eth1"), NicIrqs::default());
    }

    #[test]
    fn first_cpu_parses_lists() {
        assert_eq!(first_cpu("12"), Some(12));
        assert_eq!(first_cpu("12,44"), Some(12));
        assert_eq!(first_cpu("0-3"), Some(0));
        assert_eq!(first_cpu(""), None);
    }
}
