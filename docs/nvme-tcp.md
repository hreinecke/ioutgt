# NVMe/TCP transport (`ioutgt-nvme-tcp`)

The first production transport. This doc covers only the TCP-specific
layer; the machinery it sits on is documented elsewhere:

- thread model, reactor, slot engine, transport contract —
  [`architecture.md`](architecture.md) (§2, §4, §5.1)
- the shared gather-send harness (`StreamSender`) —
  [`stream-sender.md`](stream-sender.md)
- the shared recv byte-source (`StreamReader`) —
  [`stream-reader.md`](stream-reader.md)

State machines mirror kernel nvmet (`drivers/nvme/target/tcp.c`);
[`nvmet-comparison.md`](nvmet-comparison.md) tracks the mapping. Errors
produce C2HTermReq / NVMe status codes, never panics or silent closes.

## Handshake (control thread)

Runs on plain Tokio before the socket reaches a queue thread
(`handshake.rs`):

- ICReq validated: PFV 1.0, HPDA 0.
- ICResp advertises MAXH2CDATA = 16 MiB; HDGST/DDGST = intersection of
  host request and policy (CRC32C; on by default, `--no-hdgst` /
  `--no-ddgst` opt out).
- `read_connect` parses the first Connect capsule → qid routes the
  socket: qid 0 → admin thread, qid n → io thread `(n-1) % N`.

## Per-connection task set

`run_queue()` (`connection.rs`) is the per-connection orchestrator on
the queue thread. It joins the generic slot engine with a send list as
`NvmeTcpQueue` (`Rc<QueueCore<Sqe>>` + `SendList<SendWork>`), then
spawns the task set. The tasks never call each other — their only
rendezvous is `NvmeTcpQueue`:

```text
run_queue(QueueConn)
  ├─ NvmeTcpQueue::new        QueueCore<Sqe> + SendList<SendWork>
  ├─ ConnCtx::new_admin/new_io
  ├─ slot tasks × sqsize      loop { await_command → dispatch::execute
  │                                  → complete() }    "task per tag"
  ├─ send task                StreamSender loop (stream-sender.md)
  ├─ keep-alive watchdog      admin queue only
  ├─ traffic beacon           io queues only (TBKAS)
  └─ recv_loop                runs as the task body
```

Command lifecycle, left to right (`complete()` = `begin_respond(tag)` +
push a `SendWork::Response`):

```text
   recv_loop               NvmeTcpQueue                send task
       │                        │                          │
  ops::recv → PduDecoder        │                          │
       │  claim_tag ──────────► │                          │
       │  solicit() R2T ──────► │ ── SendWork::R2t ──────► │ encode_r2t
       │  submit(tag, sqe) ───► │                          │
       │                        │ wakes slot task `tag`:   │
       │                        │   dispatch::execute      │
       │                        │   → Backend read/write   │
       │                        │   → complete(tag)        │
       │                        │ ── SendWork::Response ─► │ encode +
       │                        │ ◄──── release_tag ────── │ gather
       │                        │                          │ sendmsg
```

The Connect SQE stashed by the control-thread handshake is the queue's
first `claim_tag()`/`submit()`. Thereafter, tag exhaustion parks the
recv loop in `await_tag` instead of terminating — a conforming host at full depth may legitimately deliver
command N+1 before our own send completion frees the tag (mirrors
nvmet; exceeding the negotiated depth is never fatal here).

## Recv: a resumable PDU phase machine

One task drives the protocol-neutral `StreamReader` (64 KiB scratch,
`fill`/`consume`, direct-into-slot tail). The NVMe phase machine stays
here; the reader has no protocol or slot knowledge. Any phase can pause
mid-PDU and resume on the next recv:

```text
 ops::recv ─► scratch buffer ─► step the phase machine:

 ┌────────┐  PduDecoder [nvme] assembles one header
 │ Header │  (headers can straddle recvs), then routes:
 └────────┘
   ├─ CapsuleCmd, no data      claim_tag, submit(tag)       ─► Header
   ├─ CapsuleCmd, host write   claim_tag, solicit() ONE R2T
   │    (transport SGL)        (TTAG = slot index)          ─► Header
   ├─ CapsuleCmd, in-capsule   claim_tag; payload is next
   │                           on the stream                ─► Data
   ├─ H2CData for a live TTAG  validate offset/length       ─► Data
   ├─ H2CTermReq from host     close WITHOUT replying
   └─ anything else            C2HTermReq, close

 ┌────────┐  memcpy scratch → slot at (PDU offset + reassembly
 │  Data  │  progress), CRC32C fused into the copy when DDGST
 └────────┘  was negotiated (skipped entirely otherwise)
   ├─ scratch drained, H2CData tail ≥ 16 KiB (H2C_DIRECT_MIN;
   │    never in-capsule, never in ring mode): one scatter
   │    recvmsg MSG_WAITALL straight into the slot's pool
   │    segments (read_direct_vectored) — no scratch→slot copy;
   │    the warm tail is folded into the CRC afterwards
   ├─ payload done, no DDGST   finish(tag)                  ─► Header
   └─ payload done, DDGST      4 digest bytes trail         ─► Ddgst

 ┌────────┐  collect the 4 trailing bytes, compare to fused CRC
 │ Ddgst  │
 └────────┘
   ├─ match                    finish(tag)                  ─► Header
   └─ mismatch                 fail THIS command only
        (DATA_XFER_ERROR|DNR, as nvmet; connection lives)   ─► Header

 finish(tag) = submit(tag) — wakes the slot task — once the full
 transfer is present. A mid-transfer H2CData just returns to Header;
 one marked `last` with bytes missing is a protocol violation
 (DATA_OUT_OF_RANGE term).
```

Three rules keep this simple and safe:

- **One outstanding recv, ever.** The direct tail is by definition the
  next bytes on the stream — the scratch recv simply isn't re-armed
  until the tail lands. Nothing reorders, nothing stalls. Measured:
  −44% target cycles/IOP on 128 KiB writes (`perf-notes.md`).
- **Failures are graded like nvmet's**: digest mismatch fails one
  command; a malformed or out-of-place PDU is a protocol violation →
  C2HTermReq (spec FES codes) and close. A command whose transfer
  exceeds MDTS terms with `DATA_LIMIT_EXCEEDED`.
- **The decoder never sees payload.** Only header bytes pass through
  `PduDecoder`; payload goes scratch → slot (or kernel → slot), keeping
  the codec modules sans-IO and the copy budget visible in one place (below).

## Send: drain everything, ship one op

The send path is the shared `StreamSender`. Each turn: block for one
work item, greedily drain the rest of the `SendList`, ship the whole
batch as a single gather `sendmsg` — headers/digests packed into a
per-batch arena, read payloads referenced in place from slot buffers,
contiguous chunks merged. The transport supplies only a staging closure
(`stage_send_work` + `release_class`, `send.rs`): encode one
`SendWork` into the arena and return its tag-release class
(`Staged::{NoRelease, AtCqe, AtNotif}`).

Two facts are load-bearing:

- **One send op in flight per connection.** Send SQEs on one socket
  carry no ordering guarantee, so the wire is never pipelined; batching
  (not op pipelining) is how the per-response park cycle was removed
  (4.2× on 4K reads; +22% on 128K reads from dropping the staging
  copy, `perf-notes.md`). This bounds *SQEs per socket* (≤ 1), not
  syscalls: the queue thread's ring still flushes all connections' SQEs
  in one `io_uring_enter` at the park.
- **`release_tag` timing is the memory-safety line.** The kernel reads
  slot pages for the whole send: release at the send CQE for
  capsule-only work (`AtCqe`), at the ZC notification for
  payload-carrying work (`AtNotif`). Teardown joins the send task,
  draining pending ZC notifications, before the queue is freed.

### Zero-copy send (`--send-zc`)

Batches go out as `SENDMSG_ZC` over the same iovecs. Double-buffered
batches keep staging through the notification RTT (≈ one host ACK);
the idle park reaps the oldest batch's notifications
(`next_work_reaping`) so tag release never depends on new work
arriving.

- **Size gate**: a batch whose average per-item payload is below
  `IOUTGT_ZC_MIN_BYTES` (default 12288) is sent as a copying `sendmsg`
  even with `--send-zc` — for small IO the per-send page-pin/IOMMU map
  costs more than the copy.
- **Vectored fixed-buffer ZC**: when the send arena is reserved from
  the registered data-pool buffer, headers and payloads share one
  `buf_index` and ship via `IORING_SEND_VECTORIZED` — no per-send page
  pin at all. A self-correcting probe disables it on kernels that
  reject it (EINVAL/EFAULT/EOPNOTSUPP).
- **`ZC_GATHER_CAP` = 1 MiB** bounds pinned pages per batch.
- **Pin-budget failures** (`RLIMIT_MEMLOCK`, ENOMEM/ENOBUFS) fall back
  to the copying send for that batch.

Why not `MSG_SPLICE_PAGES` (what nvmet uses)? Splice *donates* pages to
the skb and signals the sender nothing — fine for nvmet's fresh
per-command kernel pages, fatal for a userspace target that must
*reuse* its preallocated slots. Lending reusable memory needs a
transmit-completion signal, which is exactly `MSG_ZEROCOPY` /
`SENDMSG_ZC`'s notification. (Moot at the ABI anyway: io_uring strips
`MSG_SPLICE_PAGES` from user `msg_flags`.)

## Copy budget

The slot buffer (leased per command from the queue's `BufPool`;
128 KiB MDTS on IO queues, 8 KiB admin data cap) is the single
rendezvous for payload bytes:

```text
 Host write (H2C)                     Host read (C2H)

 kernel ──ops::recv──► scratch        backend fills the slot:
                       (64 KiB)         file read_at — O_DIRECT DMAs
    ① memcpy + fused CRC │              into slot pages, zero copy
    (in-capsule data,    │                       │
     buffered prefixes)  ▼                       ▼
   ┌──────────────┐                    ┌──────────────┐
   │ slot buffer  │ ◄══ ①' H2CData     │ slot buffer  │
   └──────────────┘     tail ≥ 16 KiB: └──────────────┘
          │             MSG_WAITALL           │ ② gather iovec
          │             straight from         │ references the slot
          ▼             the kernel            │ IN PLACE — no copy
   backend write_at                           ▼
   on the slot segs —                  sendmsg ──► kernel
   no further copy                     (--send-zc: pin, not copy)
```

| Path                                     | Copies | Notes                        |
|------------------------------------------|--------|------------------------------|
| H2C in-capsule / buffered prefix         | 1 (①)  | scratch → slot, CRC fused    |
| H2CData tail ≥ `H2C_DIRECT_MIN` (16 KiB) | 0 (①') | lands directly in the slot   |
| C2H payload                              | 0 (②)  | slot referenced by the iovec |

The one write-side copy (①) is the product, not waste: a flat
MDTS-sized lease absorbs arbitrarily fragmented TCP segments and
H2CData splits, so backends never see transport scatter — and the large
tails that dominate bulk writes skip it (①'). CRC32C always runs while
the bytes are cache-hot, never as a cold pass.

Backends add their own copies: file adds **none** (O_DIRECT is the
default, buffered only as a fallback when the store refuses it — same
default-direct policy as nvmet; the `buffered_io` mapping is in
`nvmet-comparison.md` §5); memory adds one per direction; null adds
none (reads memset the slot — visible when measuring protocol overhead
with it). The default backend is `memory`; O_DIRECT only matters under
`--backend file`. Everything else on the path is bounded per PDU:
header assembly in the decoder, the 64-byte SQE stash, and
header/digest encoding into the send arena. The data-pool arena is also registered as an io_uring fixed
buffer, so disk IO from pooled slots uses `READV_FIXED`/`WRITEV_FIXED`
(best-effort; released at teardown).

## Wire details

- **In-capsule writes**: up to 16 KiB inline (IOCCSZ = (64 + 16384)/16).
  Larger transfers via R2T, **one outstanding R2T per command** (as
  nvmet), TTAG = slot index — no TTAG lookup map exists.
- **`c2h_success` elision**: the SUCCESS flag on the final C2HData
  elides the response capsule — gated on the host disabling SQ flow
  control (Connect CATTR bit 2 → `sqhd_disabled`), not on a config
  flag, and only for clean reads.
- **Status mapping**: `BackendError` → NVMe SC via `io::nvme_status`
  (`ioutgt-nvme`), a free function mirroring nvmet's
  `blk_to_nvme_status`.

## Zero-copy receive (`--recv-buf-mb`, default off)

The alternative recv strategy: a provided-buffer multishot RECV ring
per IO connection (`ioutgt-uring/src/bufring.rs`). The kernel fills
app-registered memory as data arrives — no per-recv submission, no
scratch copy — and an H2C payload is handed to the backend
`WRITE_FIXED` straight from ring memory. Per connection it is either
this **or** the classic scratch + direct-tail path (`StreamReader` ring
mode vs classic mode); the two never mix on one connection.

```text
 ring anatomy (per ring-enabled connection):

   one arena, two fixed sub-buffers of recv_buf_mb/2 each
   ┌───────────────┬───────────────┐   IOU_PBUF_RING_INC: the kernel
   │ sub-buffer A  │ sub-buffer B  │   fills ONE sub-buffer across
   └───────────────┴───────────────┘   many CQEs → a whole H2C payload
     active recv      draining         accumulates contiguously =
     target           in-flight        exactly what a single-buf_index
                      writes           WRITE_FIXED needs
```

- **Per-connection, not shared** — a recv CQE reports `(bid, len)` but
  not the consume offset; one consumer per ring keeps the tracked
  offset authoritative. Each connection gets its own `bgid` and fixed
  buffers. (Sharing pressure is keyed on *controller* count, not queue
  count — the target offers `io_threads` IO queues per controller, a
  bijection onto the io-threads, so a shared ring was only ever at
  risk with ≥ 2 controllers; per-connection ownership makes it moot.)
- **Retain + borrow**: a payload that fits the current sub-buffer is
  retained in place (`SlotData::ring()` lease — implemented for both
  in-capsule data and whole-transfer H2CData) and the write borrows the
  sub-buffer; it is re-provided to the kernel only when recv is done
  with it and all borrows drained. A payload straddling the two
  sub-buffers falls back to the copy path — correctness never depends
  on retention.
- **Backpressure without stalls**: both sub-buffers out → multishot
  posts `-ENOBUFS` → recv parks in `wait_for_provide` (gated on a
  provide-generation snapshot taken at *arm* time, or a full
  exhaust/re-provide cycle would park forever) and wakes when a
  completing write releases its borrow. A write's completion never
  waits on recv.
- **Ceilings, graceful**: the per-thread fixed-buffer table (64 slots,
  ~3 per connection counting the pool arena) and `RLIMIT_MEMLOCK` both
  fall back to classic recv (confirm the `recv ring engaged` log
  line). Admin queues always skip it; kernels
  without `IOU_PBUF_RING_INC` (< ~6.12) fall back with a `debug!`.
- **Off by default**: memory is pre-pinned per connection
  (connections × recv_buf_mb), and the win is copy-elision + freed
  io-thread CPU, not headline throughput — measured perf-neutral for a
  single connection on real hardware; pays at high connection counts.

## Lifecycle hardening

- **Keep-alive watchdog** (admin queues only): polls every
  `keepalive_tick` = KATO/2 clamped to 250 ms..5 s; a host silent past
  KATO×2 + one tick gets `shutdown(fd)`, unwinding the whole connection
  through recv EOF (KATO 0 disables).
- **Traffic-based keep-alive** (`CTRATT.TBKAS`, advertised by this
  transport): "silent" means the whole controller, not just its admin
  queue. `NvmeTcpQueue::submit` marks a thread-local `Cell` for every
  command it takes, and each IO connection runs a *traffic beacon* task
  on the same tick that forwards the mark into the controller's shared
  `TrafficFlag` (`ioutgt-core` registry) — one relaxed store per tick
  per busy queue instead of a contended cacheline per command, and the
  only path by which an IO thread reaches its admin queue's deadline.
  The watchdog takes the flag and treats it as having been heard from,
  so a host with IO in flight stops sending Keep Alive commands
  altogether (`nvme_keep_alive_work` skips them once it sees the bit).
  An IO queue's teardown sets the flag too, granting one more period,
  as `nvmet_sq_destroy()` does.
- **Send-task death self-heals**: if the send loop ends, the spawner
  shuts the socket down so the recv loop sees EOF immediately instead
  of waiting for the host IO timeout.
- **Teardown leaks rather than use-after-frees**: intake stops, the
  send task is joined (draining ZC notifications), then the executing
  counter is quiesced; a backend op still running after the 10 s budget
  causes `run_queue` to `mem::forget` the queue instead of freeing
  memory the kernel may still touch.

## Knobs

| Flag | Default | Effect |
|------|---------|--------|
| `--backend` | `memory` | `memory` / `null` / `file` (regular file or bdev, O_DIRECT) |
| `--no-hdgst` / `--no-ddgst` | digests on | disable header/data digest negotiation |
| `--send-zc` | off | `SENDMSG_ZC` batches (see size gate above) |
| `IOUTGT_ZC_MIN_BYTES` (env) | 12288 | min average payload for a ZC batch |
| `--recv-buf-mb N` | 0 (off) | per-connection provided-buffer recv ring |
| `--io-queue-size` | 128 (max 256) | advertised IO MAXCMD/SQ depth ceiling |
| `--queue-buf-mb` | 8 | per-queue data pool (`BufPool`) size |
| `--idle-teardown-secs` | 30 | queue-thread pool reclaim grace (0 off) |

There is no `--poll` on the TCP binary — adaptive busy-poll is
RDMA-only today (`nvme-rdma.md`).
