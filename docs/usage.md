# ioutgt command-line usage

## Running the target

```sh
# Flag-driven: one subsystem, one namespace.
ioutgt --listen 0.0.0.0:4420 --io-threads 4 --backend memory --mem-size-mb 1024

# Config-driven: everything from JSON (see "Config file" below).
ioutgt --config target.json
```

| Flag | Default | Meaning |
|---|---|---|
| `--config <path>` | — | JSON config file; overrides all other target flags |
| `--listen <addr:port>` | `0.0.0.0:4420` | NVMe/TCP listen address |
| `--io-threads <n>` | `2` | IO queue threads (admin thread is implicit); also caps the queue count offered to hosts |
| `--backend <kind>` | `memory` | `memory`, `null`, or a **path** (regular file or block device, opened O_DIRECT with buffered fallback) |
| `--mem-size-mb <n>` | `64` | Namespace size for `memory`/`null` backends |
| `--subsys-nqn <nqn>` | `nqn.2026-06.io.ioutgt:test` | Subsystem NQN |
| `--no-hdgst` / `--no-ddgst` | off | Refuse header/data digest negotiation |
| `--no-pin` | pinning on | Disable topology-aware IO-thread pinning (each IO thread pins to one CPU of its `spread_cpus` group — NUMA/cluster/SMT-aware) |
| `--send-zc` | off | **Experimental.** Ship payload-carrying send batches as `SENDMSG_ZC` (zero-copy), gating slot-buffer reuse on the kernel's notification CQE. Loopback always falls back to copying — a real NIC is needed for any benefit. Startup fails if the kernel lacks `IORING_OP_SENDMSG_ZC` |
| `--control-socket <path>` | `$XDG_RUNTIME_DIR/ioutgt.sock`, else `/tmp/ioutgt.sock` | Runtime control API socket, created mode 0600 (same default as the `ctl`/`list` subcommands; config-file mode enables it only when the JSON sets `control_socket`) |

Logging via `RUST_LOG` (`tracing_subscriber` env-filter syntax):
`RUST_LOG=debug ioutgt …`, or per-module
`RUST_LOG=ioutgt_nvme_tcp=debug,info`.

The well-known discovery subsystem is always served; `nvme discover
-t tcp -a <ip> -s <port>` lists every configured subsystem.

## Config file

```json
{
  "listen": "0.0.0.0:4420",
  "io_threads": 2,
  "header_digest": true,
  "data_digest": true,
  "send_zc": false,
  "control_socket": "/tmp/ioutgt.sock",
  "subsystems": [
    {
      "nqn": "nqn.2026-06.io.ioutgt:test",
      "serial": "IOUTGT0001",
      "namespaces": [
        { "nsid": 1, "backend": { "type": "file", "path": "/var/lib/ioutgt/ns1.img" } },
        { "nsid": 2, "backend": { "type": "memory", "size_mb": 64 } },
        { "nsid": 3, "backend": { "type": "null", "size_mb": 1024 } }
      ]
    }
  ]
}
```

Per subsystem, `allow_any_host` (default true) and `allowed_hosts`
(hostnqns admitted when it is false) give nvmet-style host ACLs; per
namespace, an optional `"uuid"` pins the host-visible identity
(`/dev/disk/by-id`) instead of the derived default.

Validation runs before any thread spawns; unknown fields, duplicate or
reserved NSIDs, zero sizes, and malformed addresses are rejected with
the offending field named. A working example lives at
`testing/example-config.json`.

### nvmetcli config files

`--config` also accepts the JSON that `nvmetcli save` writes for the
kernel target (`/etc/nvmet/config.json`) — the two schemas are
auto-detected, so an existing nvmet configuration drives ioutgt
unchanged:

```sh
ioutgt --config /etc/nvmet/config.json
```

The port matching the binary's fabric (`tcp` here, `rdma` for the RDMA
binary) supplies the listen address; its exported subsystems are served
with their host ACLs, serial/model, and file/bdev-backed namespaces
(`device.path`, `device.uuid`; a namespace with `"enable": 0` stays
invisible, as in the kernel). Attributes with no ioutgt counterpart
(`param.*`, ANA groups, referrals, PI/cntlid tuning, `nguid`) are
accepted and ignored, like nvmetcli's own error-skipping restore.
Engine tuning (`io_threads`, buffer sizes, digests…) has no home in
that schema and keeps its defaults.

## Runtime control: `ioutgt ctl`

One JSON request per invocation against a running target's control
socket; the response prints on stdout and the exit code reflects
`"ok"`.

```sh
ioutgt ctl '{"op":"LIST_NAMESPACE"}'
ioutgt ctl \
    '{"op":"ADD_NAMESPACE","nsid":4,"backend":{"type":"memory","size_mb":32}}'
ioutgt ctl '{"op":"REMOVE_NAMESPACE","nsid":4}'
ioutgt ctl '{"op":"GET_STATS"}'
ioutgt ctl '{"op":"LIST_CONTROLLER"}'
ioutgt list                                     # human-readable form
```

Operations: `ADD_NAMESPACE`, `REMOVE_NAMESPACE`, `LIST_NAMESPACE`,
`LIST_CONTROLLER`, `GET_STATS`. `subsysnqn` is optional while a single
subsystem is configured. Namespace changes propagate to connected hosts
via the NS_ATTR_CHANGED async event — hosts rescan without reconnecting.
The protocol is plain newline-delimited JSON, so `nc -U` works too.

`LIST_CONTROLLER` reports each live controller's cntlid, subsystem and
host NQNs, granted KATO, installed queues — including the queue depth
the kernel tid of the serving queue thread (`top -H` / `perf -t`
friendly), and its live CPU affinity (`*` = unpinned, e.g. with `--no-pin`; by default
each IO queue shows its `spread_cpus` CPU) — plus the target
pid and the namespaces visible through the controller. The response also carries the port's
discoverable inventory (listen address, subsystems, namespaces), which
`ioutgt list` prints before the controller list — so an idle target
shows what hosts would discover rather than only `no controllers`.
(`list-ctrl` remains as an alias for `list`.)

## Counters: `ioutgt stat`

`GET_STATS` carries a `controller_info` array (which controller —
subsystem and host NQN — each cntlid below belongs to) and a `threads`
array: one entry per queue thread with its ring counters (`parks` =
idle `io_uring_enter` waits, `sqes` with its `send_sqes`/`recv_sqes`
network split and `read_sqes`/`write_sqes` backend split, `cqes`) and
per-queue IO
counters (read/write/flush/other commands, read/write bytes, errors —
IO-path failures only, admin and fabrics rejections are not counted —
keyed by cntlid+qid; correlate with `LIST_CONTROLLER` for tid/cpus).
Counts from disconnected queues fold into the thread's monotonic
`retired` totals. Counters are plain per-thread `Cell`s snapshotted
via a mailbox round trip — the IO path pays no atomics or locks for
them, and a wedged thread degrades to an `"error": "thread
unresponsive"` entry after 500 ms rather than hanging the API.
`{"op":"GET_STATS","clear":true}` zeros every counter (queues, retired,
ring) after the snapshot — the reply still carries the final totals.

```sh
ioutgt stat            # lifetime totals
ioutgt stat -i 2       # per-second rates every 2 s (iostat-style)
ioutgt stat --clear    # print the final totals, then zero everything
```

```text
controller 1: nqn.2026-06.io.ioutgt:test  host nqn.2014-08.org.nvmexpress:uuid:abc…
ioutgt-io0  tid 12345  parks/s 8011  sqes/s 282600  sqes/park 35.3  send/s 16010  recv/s 16080  read/s 250300  write/s 0  cqes/s 282700
  cntlid 1 qid 1   read 250310/s (977.8 MiB/s)  write 0/s (0.0 MiB/s)  flush 0/s  other 0/s  err 0/s
```

`sqes/park` is the park-batching amortization (SQEs per `io_uring_enter`,
i.e. ops per syscall) — shown directly, and scale-free so it reads the
same in totals and rate mode. The SQE split shows the op mix:
`send`/`recv` are the network ops (the
gather keeps `send` far below the response count), `read`/`write` are
the backend storage ops (one ring op per command on the file/bdev
backend — `0` for memory/null, which serve in-CPU); the remainder
`sqes − send − recv − read − write` is keep-alive timers + the mailbox
doorbell. Rates are computed client-side from the monotonic counters,
so a target restart
between samples shows zeros, never garbage.

## Connecting a Linux host

```sh
modprobe nvme-tcp
nvme discover -t tcp -a <target-ip> -s 4420
nvme connect  -t tcp -a <target-ip> -s 4420 -n nqn.2026-06.io.ioutgt:test \
              --nr-io-queues 4 [--hdr-digest] [--data-digest]
nvme list
…
nvme disconnect -n nqn.2026-06.io.ioutgt:test
```

## Load generator (development tool)

```sh
cargo run --release --example loadgen -- \
    --addr 127.0.0.1:14420 --conns 4 --qd 32 --bs 4096 --secs 10 --rw randread
```

Raw NVMe/TCP client on the project's own codec: pipelines `--qd`
commands per connection (`--conns` connections, one IO queue each)
and reports IOPS plus p50/p99/p999 latency. `--rw randwrite` uses
in-capsule writes for blocks ≤ 16 KiB and R2T-solicited H2CData for
larger blocks (e.g. `--bs 131072`). Intended for loopback A/B
work on the target itself — see `docs/perf-notes.md` for why fio
through the test VM is not a useful target benchmark.

## Test harnesses

```sh
cargo test --workspace            # unit + in-process integration suites
                                  #   (incl. io_verify: concurrent mixed-size
                                  #    data-integrity torture on both write paths)
testing/run_interop.sh            # full VM interop: discover/connect, fio
                                  #   --verify matrix, mkfs/mount/fstrim/fsck
testing/run_interop.sh ioutgt_fio # ONLY the fio data-integrity verify stage
                                  #   IOUTGT_BACKEND=file|null|memory
                                  #   IOUTGT_ENABLE_KILL=1  (kill/recovery test)
                                  #   IOUTGT_SOAK_ONLY=N    (reconnect-leak gate)
                                  #   IOUTGT_SOAK_CYCLES=N  (matrix soak length)
sudo testing/capture-nvmet-fixtures.sh   # optional: kernel-nvmet pcap fixtures
```

The VM harness binds port **14420** (4420 is the canonical NVMe port
and is frequently owned by other targets on a development box) and
publishes the port to the guest through the vmtest 9p marker
directory; results land in `…/vmtest/data/tmp/ioutgt_result`.
