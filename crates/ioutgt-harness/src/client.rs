//! Shared control-socket client used by the target binaries' `ctl`/`list`/`stat`
//! subcommands. Each binary defines its own `Command` enum + default socket
//! path (the names differ per transport) and dispatches into these functions.

use std::io::{BufRead, BufReader, Write};

/// Send one request line over the control socket; return the raw
/// response line (trailing newline stripped).
fn ctl_request(socket: &std::path::Path, request: &str) -> std::io::Result<String> {
    let mut stream = std::os::unix::net::UnixStream::connect(socket)?;
    stream.write_all(request.as_bytes())?;
    stream.write_all(b"\n")?;
    let mut response = String::new();
    BufReader::new(&stream).read_line(&mut response)?;
    response.truncate(response.trim_end().len());
    Ok(response)
}

/// `ioutgt ctl`: forward one JSON request verbatim, echo the raw
/// response line, exit 1 unless the server said `"ok": true`.
pub fn ctl(socket: &std::path::Path, request: &str) -> std::io::Result<()> {
    // Validate locally for a friendlier error than the server echo.
    serde_json::from_str::<serde_json::Value>(request)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let response = ctl_request(socket, request)?;
    println!("{response}");
    let parsed = serde_json::from_str::<serde_json::Value>(&response)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if parsed.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        std::process::exit(1);
    }
    Ok(())
}

/// `ioutgt list`: render the target's inventory and live controllers.
pub fn list_target(socket: &std::path::Path) -> std::io::Result<()> {
    let raw = ctl_request(socket, r#"{"op":"LIST_CONTROLLER"}"#)?;
    let response = serde_json::from_str::<serde_json::Value>(&raw)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if response.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        eprintln!("{raw}");
        std::process::exit(1);
    }
    print!("{}", render_ctrl_list(&response["data"]));
    Ok(())
}

/// `ioutgt stat`: one snapshot, or `-i N` for iostat-style rates
/// (client-side deltas of the monotonic counters — the target never
/// computes rates). `--clear` prints the final totals and zeros every
/// counter target-side.
pub fn stat_target(
    socket: &std::path::Path,
    interval: Option<u64>,
    clear: bool,
) -> std::io::Result<()> {
    let request = if clear {
        r#"{"op":"GET_STATS","clear":true}"#
    } else {
        r#"{"op":"GET_STATS"}"#
    };
    let fetch = || -> std::io::Result<serde_json::Value> {
        let raw = ctl_request(socket, request)?;
        let v: serde_json::Value = serde_json::from_str(&raw)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if v.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            return Err(std::io::Error::other(raw));
        }
        Ok(v["data"].clone())
    };
    let mut prev = fetch()?;
    print!("{}", render_stat(&prev, None));
    let Some(secs) = interval else { return Ok(()) };
    // Divide by measured elapsed, not the nominal interval: the fetch
    // itself can take a while (up to 500 ms per unresponsive thread).
    let mut prev_at = std::time::Instant::now();
    loop {
        std::thread::sleep(std::time::Duration::from_secs(secs));
        let next = fetch()?;
        let now = std::time::Instant::now();
        println!();
        print!(
            "{}",
            render_stat(&next, Some((&prev, (now - prev_at).as_secs_f64())))
        );
        prev = next;
        prev_at = now;
    }
}

/// Counter keys in the order the `read … write … flush other err` rows
/// print them (bytes follow their cmd counter).
const STAT_KEYS: [&str; 7] = [
    "read_cmds",
    "read_bytes",
    "write_cmds",
    "write_bytes",
    "flush_cmds",
    "other_cmds",
    "errors",
];

/// Render GET_STATS `data`. With `prev` = (previous snapshot, elapsed
/// seconds), counters print as per-second deltas; deltas saturate at
/// zero so a target restart between samples shows zeros, not garbage.
fn render_stat(data: &serde_json::Value, prev: Option<(&serde_json::Value, f64)>) -> String {
    use std::fmt::Write;

    fn u(v: &serde_json::Value, key: &str) -> u64 {
        v[key].as_u64().unwrap_or(0)
    }
    // Per-second (rounded) when an interval is given, identity otherwise.
    let rate = |raw: u64| -> u64 {
        match prev {
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            Some((_, secs)) if secs > 0.0 => (raw as f64 / secs).round() as u64,
            _ => raw,
        }
    };
    let val = |cur: u64, before: u64| -> u64 {
        if prev.is_some() {
            rate(cur.saturating_sub(before))
        } else {
            cur
        }
    };
    // The displayed "amount" before any per-second rounding: the interval
    // delta in rate mode, the lifetime total otherwise. Used for ratios
    // (e.g. sqes/park), where dividing two rounded rates loses precision.
    let amt = |cur: u64, before: u64| -> u64 {
        if prev.is_some() {
            cur.saturating_sub(before)
        } else {
            cur
        }
    };
    // sqes-per-park = ops submitted per io_uring_enter — the park-batching
    // amortization (how many SQEs ride each idle syscall). Scale-free, so
    // it reads the same in totals and rate mode.
    #[allow(clippy::cast_precision_loss)]
    let per = |num: u64, den: u64| -> f64 {
        if den == 0 {
            0.0
        } else {
            num as f64 / den as f64
        }
    };
    let mib = |bytes: u64| -> String {
        #[allow(clippy::cast_precision_loss)]
        let v = bytes as f64 / f64::from(1u32 << 20);
        format!("{v:.1} MiB")
    };
    let suffix = if prev.is_some() { "/s" } else { "" };

    // Shared 6-bucket batch-size histogram: `<prefix>_b1 … <prefix>_b32`
    // rendered as "label:val …" (backend rw/submit, RDMA WR batch rows).
    const BUCKETS: [(&str, &str); 6] = [
        ("b1", "1"),
        ("b2", "2"),
        ("b4", "<=4"),
        ("b8", "<=8"),
        ("b16", "<=16"),
        ("b32", ">16"),
    ];
    let hist = |src: &serde_json::Value, src0: &serde_json::Value, prefix: &str| -> String {
        BUCKETS
            .iter()
            .map(|(bucket, label)| {
                let key = format!("{prefix}_{bucket}");
                format!("{label}:{}", val(u(src, &key), u(src0, &key)))
            })
            .collect::<Vec<_>>()
            .join(" ")
    };

    let find_thread = |name: &str| -> Option<&serde_json::Value> {
        prev?.0["threads"]
            .as_array()?
            .iter()
            .find(|t| t["name"] == name)
    };
    let match_queue = |t: &serde_json::Value, q: &serde_json::Value| -> serde_json::Value {
        t["queues"]
            .as_array()
            .and_then(|qs| {
                qs.iter()
                    .find(|p| p["cntlid"] == q["cntlid"] && p["qid"] == q["qid"])
            })
            .cloned()
            .unwrap_or_default()
    };
    // Live queues + retired: monotonic per thread (a retiring queue's
    // counts move, they never vanish).
    let thread_total = |t: &serde_json::Value, key: &str| -> u64 {
        let live: u64 = t["queues"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|q| u(q, key))
            .sum();
        live + u(&t["retired"], key)
    };

    let mut out = String::new();
    // Identity first: which controller/subsystem/host each cntlid in
    // the per-queue rows below belongs to.
    for c in data["controller_info"].as_array().into_iter().flatten() {
        let kind = if c["discovery"] == true {
            " (discovery)"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "controller {}{kind}: {}  host {}",
            c["cntlid"],
            c["subsysnqn"].as_str().unwrap_or("?"),
            c["hostnqn"].as_str().unwrap_or("?"),
        );
    }
    for thread in data["threads"].as_array().into_iter().flatten() {
        if let Some(err) = thread["error"].as_str() {
            let _ = writeln!(
                out,
                "thread {}: {err}",
                thread["name"].as_str().unwrap_or("?")
            );
            continue;
        }
        let name = thread["name"].as_str().unwrap_or("?");
        let before = find_thread(name).cloned().unwrap_or_default();
        let ring = &thread["ring"];
        let ring0 = &before["ring"];
        let _ = writeln!(
            out,
            "{name}  tid {}  parks{suffix} {}  sqes{suffix} {}  sqes/park {:.1}  send{suffix} {}  recv{suffix} {}  read{suffix} {}  write{suffix} {}  cqes{suffix} {}",
            thread["tid"],
            val(u(ring, "parks"), u(ring0, "parks")),
            val(u(ring, "sqes"), u(ring0, "sqes")),
            per(
                amt(u(ring, "sqes"), u(ring0, "sqes")),
                amt(u(ring, "parks"), u(ring0, "parks")),
            ),
            val(u(ring, "send_sqes"), u(ring0, "send_sqes")),
            val(u(ring, "recv_sqes"), u(ring0, "recv_sqes")),
            val(u(ring, "read_sqes"), u(ring0, "read_sqes")),
            val(u(ring, "write_sqes"), u(ring0, "write_sqes")),
            val(u(ring, "cqes"), u(ring0, "cqes")),
        );
        // Backend-IO submission batching (present once the reactor exports
        // it): how many storage read/write SQEs each io_uring_enter carried.
        // Suppressed when the thread did no backend ring IO (memory/null
        // backends, idle threads).
        let rw_total: u64 = BUCKETS
            .iter()
            .map(|(bucket, _)| {
                let key = format!("rw_sq_{bucket}");
                amt(u(ring, &key), u(ring0, &key))
            })
            .sum();
        if rw_total > 0 {
            let _ = writeln!(out, "  backend rw/submit [{}]", hist(ring, ring0, "rw_sq"));
        }
        for q in thread["queues"].as_array().into_iter().flatten() {
            let q0 = match_queue(&before, q);
            let _ = writeln!(
                out,
                "  cntlid {} qid {}   read {}{suffix} ({}{suffix})  write {}{suffix} \
                 ({}{suffix})  flush {}{suffix}  other {}{suffix}  err {}{suffix}",
                q["cntlid"],
                q["qid"],
                val(u(q, "read_cmds"), u(&q0, "read_cmds")),
                mib(val(u(q, "read_bytes"), u(&q0, "read_bytes"))),
                val(u(q, "write_cmds"), u(&q0, "write_cmds")),
                mib(val(u(q, "write_bytes"), u(&q0, "write_bytes"))),
                val(u(q, "flush_cmds"), u(&q0, "flush_cmds")),
                val(u(q, "other_cmds"), u(&q0, "other_cmds")),
                val(u(q, "errors"), u(&q0, "errors")),
            );
            // Transport WR row (RDMA only): completion rate + live inflight per
            // class. A stuck-high read inflight with a zero read rate is the
            // signature of RDMA READs posted but not completing.
            if let Some(wr) = q.get("wr").filter(|w| w.is_object()) {
                let wr0 = q0.get("wr").cloned().unwrap_or_default();
                let done_amt = amt(u(wr, "read_done"), u(&wr0, "read_done"))
                    + amt(u(wr, "write_done"), u(&wr0, "write_done"))
                    + amt(u(wr, "send_done"), u(&wr0, "send_done"))
                    + amt(u(wr, "recv_done"), u(&wr0, "recv_done"));
                // Send-queue WRs posted this interval (READ+WRITE+SEND) over the
                // doorbells rung = submission batch size (1.0 = no WR chaining).
                let sq_posted_amt = amt(u(wr, "read_posted"), u(&wr0, "read_posted"))
                    + amt(u(wr, "write_posted"), u(&wr0, "write_posted"))
                    + amt(u(wr, "send_posted"), u(&wr0, "send_posted"));
                let _ = writeln!(
                    out,
                    "    wr  read {}{suffix} if {}  write {}{suffix} if {}  \
                     send {}{suffix} if {}  recv {}{suffix} if {}  poll {}{suffix} ({:.1}/poll)  \
                     db {}{suffix} ({:.1} wr/db)",
                    val(u(wr, "read_done"), u(&wr0, "read_done")),
                    u(wr, "read_inflight"),
                    val(u(wr, "write_done"), u(&wr0, "write_done")),
                    u(wr, "write_inflight"),
                    val(u(wr, "send_done"), u(&wr0, "send_done")),
                    u(wr, "send_inflight"),
                    val(u(wr, "recv_done"), u(&wr0, "recv_done")),
                    u(wr, "recv_inflight"),
                    val(u(wr, "poll_batches"), u(&wr0, "poll_batches")),
                    per(
                        done_amt,
                        amt(u(wr, "poll_batches"), u(&wr0, "poll_batches"))
                    ),
                    val(u(wr, "sq_doorbells"), u(&wr0, "sq_doorbells")),
                    per(
                        sq_posted_amt,
                        amt(u(wr, "sq_doorbells"), u(&wr0, "sq_doorbells"))
                    ),
                );
                // Batch-size distributions (present once the transport exports
                // them): how many WRs shared each doorbell and how many CQEs
                // each non-empty poll reaped — the averages above can hide a
                // bimodal mix (many 1-WR doorbells + a few huge ones).
                if wr.get("poll_b1").is_some() {
                    let _ = writeln!(
                        out,
                        "    batch  read/db [{}]  resp/db [{}]  recv/db [{}]  cqe/poll [{}]",
                        hist(wr, &wr0, "read_db"),
                        hist(wr, &wr0, "resp_db"),
                        hist(wr, &wr0, "recv_db"),
                        hist(wr, &wr0, "poll"),
                    );
                }
            }
        }
        // Retired row. In rate mode, diffing `retired` alone would
        // re-report a mid-interval-retired queue's whole lifetime as one
        // interval's "rate"; instead diff the monotonic thread total and
        // attribute whatever the live rows above did not already show.
        let r: Vec<u64> = if prev.is_some() {
            STAT_KEYS
                .iter()
                .map(|key| {
                    let total_delta =
                        thread_total(thread, key).saturating_sub(thread_total(&before, key));
                    let shown: u64 = thread["queues"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|q| u(q, key).saturating_sub(u(&match_queue(&before, q), key)))
                        .sum();
                    rate(total_delta.saturating_sub(shown))
                })
                .collect()
        } else {
            STAT_KEYS
                .iter()
                .map(|key| u(&thread["retired"], key))
                .collect()
        };
        let any_retired = STAT_KEYS
            .iter()
            .zip(&r)
            .any(|(key, v)| *v > 0 && !key.ends_with("_bytes"));
        if any_retired {
            let _ = writeln!(
                out,
                "  retired          read {}{suffix} ({}{suffix})  write {}{suffix} \
                 ({}{suffix})  flush {}{suffix}  other {}{suffix}  err {}{suffix}",
                r[0],
                mib(r[1]),
                r[2],
                mib(r[3]),
                r[4],
                r[5],
                r[6],
            );
        }
    }
    out
}

/// Expand a kernel cpulist (`"0-1,32-33"`) into individual CPU ids.
fn expand_cpulist(s: &str) -> Vec<u32> {
    let mut cpus = Vec::new();
    for part in s.split(',') {
        match part.split_once('-') {
            Some((a, b)) => {
                if let (Ok(a), Ok(b)) = (a.trim().parse::<u32>(), b.trim().parse::<u32>()) {
                    cpus.extend(a..=b);
                }
            }
            None => {
                if let Ok(c) = part.trim().parse::<u32>() {
                    cpus.push(c);
                }
            }
        }
    }
    cpus
}

/// Render a queue thread's CPU placement: the full online `group` (all CPUs the
/// thread may use), with the `active` (pinned) CPU bracketed. Falls back to just
/// `active` when the group is unknown/unpinned (`"*"`).
fn render_cpu_group(group: &str, active: &str) -> String {
    if group == "*" || group.is_empty() {
        return active.to_owned();
    }
    let act = active.parse::<u32>().ok();
    expand_cpulist(group)
        .into_iter()
        .map(|c| {
            if Some(c) == act {
                format!("[{c}]")
            } else {
                c.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// One block per controller; NQNs are too long for fixed columns.
fn render_ctrl_list(data: &serde_json::Value) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(out, "pid {}", data["pid"]);
    // Discoverable inventory (bound ports + subsystems), shown in
    // every state; skipped silently if the server predates it.
    for port in data["ports"].as_array().into_iter().flatten() {
        let _ = writeln!(
            out,
            "port {}:{}",
            port["traddr"].as_str().unwrap_or("?"),
            port["trsvcid"].as_str().unwrap_or("?")
        );
        for subsys in port["subsystems"].as_array().into_iter().flatten() {
            let _ = writeln!(out, "  subsystem {}", subsys["nqn"].as_str().unwrap_or("?"));
            for ns in subsys["namespaces"].as_array().into_iter().flatten() {
                let blocks = ns["blocks"].as_u64().unwrap_or(0);
                let shift = u32::try_from(ns["block_shift"].as_u64().unwrap_or(0).min(63))
                    .expect("bounded by min(63)");
                let bytes = blocks << shift;
                const GIB: u64 = 1 << 30;
                let size = if bytes > 0 && bytes % GIB == 0 {
                    format!("{} GiB", bytes / GIB)
                } else {
                    format!("{} MiB", bytes >> 20)
                };
                let _ = writeln!(
                    out,
                    "    ns {}: {size} ({}B blocks)",
                    ns["nsid"],
                    1u64 << shift
                );
            }
        }
    }
    let controllers = data["controllers"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    if controllers.is_empty() {
        out.push_str("no controllers\n");
        return out;
    }
    for c in controllers {
        let kind = if c["discovery"] == true {
            " (discovery)"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "controller {}{kind}: {}",
            c["cntlid"],
            c["subsysnqn"].as_str().unwrap_or("?")
        );
        let _ = writeln!(out, "  host:   {}", c["hostnqn"].as_str().unwrap_or("?"));
        let _ = writeln!(out, "  kato:   {} ms", c["kato_ms"]);
        let queues = c["queues"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|q| {
                let active = q["cpus"].as_str().unwrap_or("?");
                let group = q["group_cpus"].as_str().unwrap_or("*");
                format!(
                    "{}:{}@{} cpus {}",
                    q["qid"],
                    q["depth"],
                    q["tid"],
                    render_cpu_group(group, active),
                )
            })
            .collect::<Vec<_>>()
            .join(" | ");
        let _ = writeln!(out, "  queues: {queues}");
        let nsids = c["namespaces"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|ns| ns["nsid"].to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(
            out,
            "  ns:     {}",
            if nsids.is_empty() {
                "-".to_owned()
            } else {
                nsids
            }
        );
    }
    out
}

#[cfg(test)]
mod tests {
    fn sample_port() -> serde_json::Value {
        serde_json::json!({
            "traddr": "0.0.0.0",
            "trsvcid": "14420",
            "subsystems": [{
                "nqn": "nqn.2026-06.io.ioutgt:test",
                "namespaces": [{"nsid": 1, "blocks": 131072, "block_shift": 9}],
            }],
        })
    }

    const PORT_HEADER: &str = "port 0.0.0.0:14420\n\
         \x20 subsystem nqn.2026-06.io.ioutgt:test\n\
         \x20   ns 1: 64 MiB (512B blocks)\n";

    #[test]
    fn render_cpu_group_marks_active() {
        // Full group dumped, active CPU bracketed.
        assert_eq!(super::render_cpu_group("0-1,32-33", "32"), "0,1,[32],33");
        assert_eq!(super::render_cpu_group("12", "12"), "[12]");
        // Unknown/unpinned group falls back to just the active CPU.
        assert_eq!(super::render_cpu_group("*", "3"), "3");
        assert_eq!(super::render_cpu_group("*", "*"), "*");
    }

    #[test]
    fn render_ctrl_list_formats_controllers() {
        let data = serde_json::json!({
            "pid": 4242,
            "ports": [sample_port()],
            "controllers": [{
                "cntlid": 1,
                "subsysnqn": "nqn.2026-06.io.ioutgt:test",
                "hostnqn": "nqn.2014-08.org.nvmexpress:uuid:abc",
                "discovery": false,
                "kato_ms": 60000,
                "queues": [
                    {"qid": 0, "depth": 32, "tid": 100, "cpus": "*"},
                    {"qid": 1, "depth": 64, "tid": 101, "cpus": "3"},
                ],
                "namespaces": [{"nsid": 1, "blocks": 32768, "block_shift": 9}],
            }],
        });
        let out = super::render_ctrl_list(&data);
        let expected = format!(
            "pid 4242\n{PORT_HEADER}\
             controller 1: nqn.2026-06.io.ioutgt:test\n\
             \x20 host:   nqn.2014-08.org.nvmexpress:uuid:abc\n\
             \x20 kato:   60000 ms\n\
             \x20 queues: 0:32@100 cpus * | 1:64@101 cpus 3\n\
             \x20 ns:     1\n"
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn render_ctrl_list_empty() {
        let data = serde_json::json!({ "pid": 4242, "ports": [sample_port()], "controllers": [] });
        assert_eq!(
            super::render_ctrl_list(&data),
            format!("pid 4242\n{PORT_HEADER}no controllers\n")
        );
    }

    #[test]
    fn render_ctrl_list_gib_sizes() {
        let data = serde_json::json!({
            "pid": 1,
            "ports": [{
                "traddr": "::", "trsvcid": "14420",
                "subsystems": [{
                    "nqn": "nqn.x",
                    // 2 GiB in 4096B blocks.
                    "namespaces": [{"nsid": 7, "blocks": 524288, "block_shift": 12}],
                }],
            }],
            "controllers": [],
        });
        let out = super::render_ctrl_list(&data);
        assert!(out.contains("port :::14420\n"), "{out}");
        assert!(out.contains("ns 7: 2 GiB (4096B blocks)\n"), "{out}");
    }

    #[test]
    fn render_ctrl_list_without_port_section() {
        let data = serde_json::json!({ "pid": 4242, "controllers": [] });
        assert_eq!(super::render_ctrl_list(&data), "pid 4242\nno controllers\n");
    }

    #[test]
    fn render_ctrl_list_discovery() {
        let data = serde_json::json!({
            "pid": 4242,
            "controllers": [{
                "cntlid": 2,
                "subsysnqn": "nqn.2014-08.org.nvmexpress.discovery",
                "hostnqn": "nqn.2014-08.org.nvmexpress:uuid:abc",
                "discovery": true,
                "kato_ms": 120000,
                "queues": [{"qid": 0, "depth": 32, "tid": 100, "cpus": "*"}],
                "namespaces": [],
            }],
        });
        let out = super::render_ctrl_list(&data);
        assert_eq!(
            out,
            "pid 4242\n\
             controller 2 (discovery): nqn.2014-08.org.nvmexpress.discovery\n\
             \x20 host:   nqn.2014-08.org.nvmexpress:uuid:abc\n\
             \x20 kato:   120000 ms\n\
             \x20 queues: 0:32@100 cpus *\n\
             \x20 ns:     -\n"
        );
    }

    fn stat_sample() -> serde_json::Value {
        serde_json::json!({
        "controller_info": [
            { "cntlid": 1, "subsysnqn": "nqn.2026-06.io.ioutgt:test",
              "hostnqn": "nqn.2014-08.org.nvmexpress:uuid:abc",
              "discovery": false },
            { "cntlid": 2, "subsysnqn": "nqn.2014-08.org.nvmexpress.discovery",
              "hostnqn": "nqn.2014-08.org.nvmexpress:uuid:abc",
              "discovery": true },
        ],
        "threads": [{
            "name": "ioutgt-io0", "tid": 42,
            "ring": { "parks": 90, "sqes": 5000, "send_sqes": 2500,
                      "rw_sq_b1": 30u64, "rw_sq_b2": 40u64, "rw_sq_b4": 15u64,
                      "rw_sq_b8": 5u64, "rw_sq_b16": 0u64, "rw_sq_b32": 0u64,
                      "recv_sqes": 2400, "read_sqes": 60, "write_sqes": 40,
                      "cqes": 5000 },
            "queues": [{ "cntlid": 1, "qid": 1,
                "read_cmds": 3000u64, "write_cmds": 1000u64, "flush_cmds": 0u64,
                "other_cmds": 2u64, "read_bytes": 12_288_000u64,
                "write_bytes": 4_096_000u64, "errors": 0u64 }],
            "retired": { "read_cmds": 0, "write_cmds": 0, "flush_cmds": 0,
                "other_cmds": 0, "read_bytes": 0, "write_bytes": 0, "errors": 0 },
        }]})
    }

    #[test]
    fn render_stat_shows_wr_row() {
        let mut data = stat_sample();
        data["threads"][0]["queues"][0]["wr"] = serde_json::json!({
            "read_posted": 100u64, "read_done": 90u64, "read_inflight": 10u64,
            "write_posted": 0u64, "write_done": 0u64, "write_inflight": 0u64,
            "send_posted": 90u64, "send_done": 90u64, "send_inflight": 0u64,
            "recv_posted": 128u64, "recv_done": 90u64, "recv_inflight": 38u64,
            "poll_batches": 45u64, "sq_doorbells": 190u64,
        });
        let out = super::render_stat(&data, None);
        assert!(out.contains("wr  read 90 if 10"), "{out}");
        assert!(out.contains("recv 90 if 38"), "{out}");
        assert!(out.contains("poll 45"), "{out}");
        // 190 send-queue WRs (read 100 + send 90) over 190 doorbells = 1.0 wr/db.
        assert!(out.contains("db 190 (1.0 wr/db)"), "{out}");
        // No batch histogram keys -> no batch row.
        assert!(!out.contains("batch  read/db"), "{out}");
    }

    #[test]
    fn render_stat_shows_batch_histograms() {
        let mut data = stat_sample();
        data["threads"][0]["queues"][0]["wr"] = serde_json::json!({
            "read_posted": 100u64, "read_done": 90u64, "read_inflight": 10u64,
            "write_posted": 0u64, "write_done": 0u64, "write_inflight": 0u64,
            "send_posted": 90u64, "send_done": 90u64, "send_inflight": 0u64,
            "recv_posted": 128u64, "recv_done": 90u64, "recv_inflight": 38u64,
            "poll_batches": 45u64, "sq_doorbells": 190u64,
            "read_db_b1": 60u64, "read_db_b2": 20u64, "read_db_b4": 0u64,
            "read_db_b8": 0u64, "read_db_b16": 0u64, "read_db_b32": 0u64,
            "resp_db_b1": 50u64, "resp_db_b2": 10u64, "resp_db_b4": 5u64,
            "resp_db_b8": 0u64, "resp_db_b16": 0u64, "resp_db_b32": 0u64,
            "recv_db_b1": 40u64, "recv_db_b2": 25u64, "recv_db_b4": 0u64,
            "recv_db_b8": 0u64, "recv_db_b16": 0u64, "recv_db_b32": 0u64,
            "poll_b1": 10u64, "poll_b2": 15u64, "poll_b4": 12u64,
            "poll_b8": 8u64, "poll_b16": 0u64, "poll_b32": 0u64,
        });
        let out = super::render_stat(&data, None);
        assert!(
            out.contains("batch  read/db [1:60 2:20 <=4:0 <=8:0 <=16:0 >16:0]"),
            "{out}"
        );
        assert!(out.contains("resp/db [1:50 2:10 <=4:5"), "{out}");
        assert!(out.contains("recv/db [1:40 2:25"), "{out}");
        assert!(out.contains("cqe/poll [1:10 2:15 <=4:12 <=8:8"), "{out}");
    }

    #[test]
    fn render_stat_totals() {
        let out = super::render_stat(&stat_sample(), None);
        // Controller identity first, so the cntlid rows are readable.
        assert!(
            out.starts_with("controller 1: nqn.2026-06.io.ioutgt:test"),
            "{out}"
        );
        // Backend-IO submission batching row from the ring histogram keys.
        assert!(
            out.contains("backend rw/submit [1:30 2:40 <=4:15 <=8:5 <=16:0 >16:0]"),
            "{out}"
        );
        assert!(
            out.contains("host nqn.2014-08.org.nvmexpress:uuid:abc"),
            "{out}"
        );
        assert!(out.contains("controller 2 (discovery):"), "{out}");
        assert!(out.contains("ioutgt-io0"), "{out}");
        assert!(out.contains("tid 42"), "{out}");
        assert!(out.contains("5000"), "sqes visible: {out}");
        // sqes/park amortization: 5000 / 90 = 55.6.
        assert!(out.contains("sqes/park 55.6"), "amortization: {out}");
        assert!(out.contains("cntlid 1 qid 1"), "{out}");
        assert!(out.contains("read 3000"), "{out}");
    }

    #[test]
    fn render_stat_interval_rates() {
        let prev = stat_sample();
        let mut next = stat_sample();
        next["threads"][0]["queues"][0]["read_cmds"] = 5000.into();
        next["threads"][0]["ring"]["parks"] = 290.into();
        // 2000 reads over 2 s → 1000/s; 200 parks over 2 s → 100/s.
        let out = super::render_stat(&next, Some((&prev, 2.0)));
        assert!(out.contains("read 1000"), "rate visible: {out}");
        assert!(out.contains("parks/s 100"), "park rate visible: {out}");
        // Counters that did not move render as zero rates, not totals.
        assert!(out.contains("write 0"), "{out}");
    }

    #[test]
    fn render_stat_saturates_on_restart() {
        // Target restarted between samples: counters went backwards.
        let prev = stat_sample();
        let mut next = stat_sample();
        next["threads"][0]["queues"][0]["read_cmds"] = 10.into();
        let out = super::render_stat(&next, Some((&prev, 1.0)));
        assert!(out.contains("read 0"), "saturating delta: {out}");
    }

    #[test]
    fn render_stat_retired_rate_is_interval_work_only() {
        // A queue retired between samples: its lifetime counts moved
        // from queues[] into retired. Only work done *this interval*
        // may show as a retired rate — never the re-folded history.
        let prev = stat_sample(); // queue live: 3000 reads, retired 0
        let mut next = stat_sample();
        next["threads"][0]["queues"] = serde_json::json!([]);
        // 3000 historical + 500 new reads folded on teardown.
        next["threads"][0]["retired"]["read_cmds"] = 3500.into();
        next["threads"][0]["retired"]["read_bytes"] = 14_336_000u64.into();
        let out = super::render_stat(&next, Some((&prev, 2.0)));
        // (3500 total - 3000 already shown) / 2 s = 250/s, not 1750/s.
        assert!(out.contains("retired"), "{out}");
        assert!(out.contains("read 250"), "interval work only: {out}");

        // No new work at all → no retired row in rate mode.
        let mut idle = stat_sample();
        idle["threads"][0]["queues"] = serde_json::json!([]);
        idle["threads"][0]["retired"]["read_cmds"] = 3000.into();
        idle["threads"][0]["retired"]["read_bytes"] = 12_288_000u64.into();
        let out = super::render_stat(&idle, Some((&prev, 2.0)));
        assert!(!out.contains("retired"), "{out}");
    }

    #[test]
    fn render_stat_unresponsive_thread() {
        let v = serde_json::json!({ "threads": [{ "error": "thread unresponsive" }] });
        let out = super::render_stat(&v, None);
        assert!(out.contains("unresponsive"), "{out}");
    }

    #[test]
    fn render_stat_skips_retired_when_zero() {
        let out = super::render_stat(&stat_sample(), None);
        assert!(!out.contains("retired"), "{out}");
        let mut v = stat_sample();
        v["threads"][0]["retired"]["write_cmds"] = 7.into();
        let out = super::render_stat(&v, None);
        assert!(out.contains("retired"), "{out}");
    }
}
