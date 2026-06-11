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
| `--pin` | off | Pin queue threads to sequential cores |
| `--control-socket <path>` | — | Enable the runtime control API on this Unix socket |

Logging via `RUST_LOG` (`tracing_subscriber` env-filter syntax):
`RUST_LOG=debug ioutgt …`, or per-module
`RUST_LOG=ioutgt_tcp=debug,info`.

The well-known discovery subsystem is always served; `nvme discover
-t tcp -a <ip> -s <port>` lists every configured subsystem.

## Config file

```json
{
  "listen": "0.0.0.0:4420",
  "io_threads": 2,
  "header_digest": true,
  "data_digest": true,
  "pin_threads": false,
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

Validation runs before any thread spawns; unknown fields, duplicate or
reserved NSIDs, zero sizes, and malformed addresses are rejected with
the offending field named. A working example lives at
`testing/example-config.json`.

## Runtime control: `ioutgt ctl`

One JSON request per invocation against a running target's control
socket; the response prints on stdout and the exit code reflects
`"ok"`.

```sh
ioutgt ctl --socket /tmp/ioutgt.sock '{"op":"LIST_NAMESPACE"}'
ioutgt ctl --socket /tmp/ioutgt.sock \
    '{"op":"ADD_NAMESPACE","nsid":4,"backend":{"type":"memory","size_mb":32}}'
ioutgt ctl --socket /tmp/ioutgt.sock '{"op":"REMOVE_NAMESPACE","nsid":4}'
ioutgt ctl --socket /tmp/ioutgt.sock '{"op":"GET_STATS"}'
```

Operations: `ADD_NAMESPACE`, `REMOVE_NAMESPACE`, `LIST_NAMESPACE`,
`GET_STATS`. `subsysnqn` is optional while a single subsystem is
configured. Namespace changes propagate to connected hosts via the
NS_ATTR_CHANGED async event — hosts rescan without reconnecting. The
protocol is plain newline-delimited JSON, so `nc -U` works too.

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
in-capsule writes (≤ 16 KiB block sizes). Intended for loopback A/B
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
