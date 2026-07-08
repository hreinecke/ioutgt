# StreamReader — the recv byte-source for stream transports

> The type is named **`StreamReader`** and lives in the `ioutgt-stream`
> crate (`crates/ioutgt-stream/src/reader.rs`), beside its send-path
> sibling [`StreamSender`](stream-sender.md). NVMe/TCP is its first user;
> NBD is the planned second.

`StreamReader` is the **recv path's byte plumbing**: it owns the socket
fd and a single scratch buffer, issues the `recv` ops, and pulls large
payloads straight into a destination buffer with the kernel's
`MSG_WAITALL` short-read loop. It is **protocol-neutral and slot-blind**:
it deals in raw byte windows and a caller-supplied destination pointer
only. Everything NVMe-specific — framing PDUs, decoding headers,
verifying digests, R2T reassembly — stays in the transport, decoding out
of the reader's window.

This document goes top to bottom: why recv abstracts *differently* from
send → where it sits → the data structure → the two mechanics → the
digest seam → safety and cancellation → how NVMe/TCP wires it up →
invariants → the asymmetry with `StreamSender`.

---

## 1. Motivation: why recv is not the mirror of send

The naive recv path mixes two concerns in one loop:

```text
  loop:
      recv(socket, buf)                 ← byte plumbing
      decode header out of buf          ← protocol
      copy payload buf → slot, CRC      ← byte plumbing + protocol
      if large tail: recv straight into slot (MSG_WAITALL)  ← byte plumbing
      dispatch                          ← protocol
```

The byte-plumbing half is identical for any stream transport: own a
scratch buffer, issue `recv`, hand out a window, and — for a large
payload that would otherwise bounce through the scratch buffer twice —
receive it straight into the destination. That last path is the
**bug-prone** part: a raw pointer into a caller's buffer, a best-effort
`MSG_WAITALL` that can return short, and a cancellation/orphan-safety
contract across an `await`. It is the recv counterpart to the send
harness's zero-copy machinery, and the strongest reason to factor it out
once.

### Why the seam is an *object*, not a closure

`StreamSender` reduces its whole protocol surface to a one-line staging
closure because the send path is **push-based**: drain work → stage →
ship → release. Each work item is independent; the harness drives the
loop and calls back for staging.

Recv is **pull-based**: read bytes → decode a header → *branch* into
payload / digest / dispatch, where the branches are themselves protocol
decisions (PDU type, HDGST/DDGST, R2T solicitation, ZC `await_tag`
backpressure). That control flow cannot be a callback the harness
drives — it *is* the transport. So the reusable seam is the inverse
shape: a **byte-source the transport pulls from**, not a driver that
calls the transport.

A direct consequence: unlike `StreamSender` — which is generic over the
slot context `C` and work type `W` and writes into `SlotArray<C>` —
`StreamReader` touches **no slots and no protocol types at all**. It does
not even depend on `ioutgt-core`. It is the smaller, simpler half.

---

## 2. Where it sits

`ioutgt-stream` bridges the two opposite leaves; the reader uses only the
io_uring leaf:

```text
        ioutgt-nvme-tcp        ← transport: PDU phase machine pulls bytes
              │  uses              from the reader, decodes, dispatches
              ▼
        ┌───────────────┐
        │ ioutgt-stream │      ← StreamReader: fd + scratch buffer +
        │  StreamReader │        recv ops + direct-into-dst MSG_WAITALL
        └───────────────┘
              │  uses
              ▼
        ioutgt-uring          ← ops::recv (owned buffer)
        · BufOp                  ops::recv_raw_waitall (raw ptr, MSG_WAITALL)
        · RawOp                  the reactor

  (note: no edge to ioutgt-core — the reader has no slot knowledge,
   unlike StreamSender, which borrows SlotArray<C>/SendList<W>.)
```

### The reader vs the phase machine

The transport owns the framing; the reader owns the bytes. The boundary
is exactly the window:

```text
   socket ──► StreamReader ──► byte window ──► RecvPhase machine [nvme-tcp]
              · fill/consume      (a &[u8])     · PduDecoder (headers)
              · read_direct  ◄─── dst ptr ──────· digests, R2T, dispatch
                (large tail straight into a slot buffer)
```

`fill` hands the phase machine a window to decode headers and small
payloads out of; for a large payload tail the phase machine hands the
reader a destination pointer and lets `read_direct` fill it. The reader
never learns what those bytes mean.

---

## 3. The data structure

The whole reader is an fd, one scratch buffer, and a consumed-prefix
cursor:

```rust
pub struct StreamReader {
    fd: i32,
    buf: Option<Box<[u8]>>, // scratch; None only across the recv await in fill()
    filled: usize,          // bytes valid after the last recv
    pos: usize,             // consumed prefix; live window = buf[pos..filled]
}
```

The window model:

```text
  buf:  [ ──────── filled ──────── ........ cap ........ ]
         ▲         ▲
         pos       filled
         └ window ─┘   = buf[pos..filled], the unconsumed bytes
```

- `fill()` refills only when the window is empty (`pos == filled`); it
  takes the buffer (the `recv` op owns it), issues one `recv`, restores
  it, and resets the cursor to `0..n`.
- `consume(n)` advances `pos`; the next `fill()` returns the shrunk
  window without a syscall until it is fully drained.

`buf` is `Option` because `ops::recv` is an *owned-buffer* op: per the
reactor's buffer-ownership rule, the buffer moves into the op and is
handed back on completion. It is `None` only for the duration of that
one `await`.

---

## 4. The two mechanics

### 4.1 `fill` / `consume` — the windowed path

```rust
pub async fn fill(&mut self) -> io::Result<&[u8]>; // recv if empty; &[] = EOF
pub fn consume(&mut self, n: usize);               // advance past processed bytes
```

```text
  fill() ──► pos == filled ?
              ├─ yes → take buf ─ ops::recv ─ restore buf ─ pos=0, filled=n
              │         (n == 0 → window is empty → caller sees EOF)
              └─ no  → (window already has bytes)
            return buf[pos..filled]

  consume(k) ──► pos += k    (next fill returns buf[pos..filled], no recv)
```

The transport decodes one or more headers (and in-capsule / small
payloads) out of the window, calling `consume` for what it processed.
A header or payload that straddles two recvs is handled by the
transport's own resumable state — the reader just keeps returning the
next window.

### 4.2 `read_direct` — the large-payload tail, straight into a slot

When a payload tail is large enough that bouncing it through the scratch
buffer would cost a second copy, the transport hands the reader a
destination pointer and lets the kernel write there directly:

```rust
/// # Safety: dst valid & unaliased for `len` writes, outlives the await (§6).
pub async unsafe fn read_direct(
    &mut self,
    dst: *mut u8,
    len: u32,
    mut on_chunk: impl FnMut(&[u8]),   // the digest seam (§5)
) -> io::Result<u32>;                  // bytes received; < len ⇒ mid-payload EOF
```

```text
  read_direct(dst, len, on_chunk)   [precondition: window empty]
     done = 0
     while done < len:
         n = recv_raw_waitall(fd, dst + done, len - done).await   ← MSG_WAITALL
         if n == 0: break                       ← orderly close mid-payload
         on_chunk(dst[done .. done+n])          ← per-completion callback
         done += n
     return done
```

`MSG_WAITALL` asks the kernel to hold the op until `len` bytes arrive, so
the common case is **one** completion for the whole tail; the loop exists
only to resume a short-but-nonzero return *in place* at the advanced
offset, and to surface a genuine EOF (`n == 0`) as a short total the
caller maps to a clean close.

The precondition — the windowed buffer must be empty — holds because the
transport only takes the direct path after it has drained the current
window; a `debug_assert!(pos == filled)` documents it.

---

## 5. The digest seam

The reader copies/receives the bytes; the transport owns any checksum
over them. The `on_chunk: impl FnMut(&[u8])` closure bridges the two
without the reader knowing what a digest is:

- It is **monomorphized**, so a transport with no digest passes
  `|_| {}` and the optimizer deletes it entirely — zero cost.
- It fires **once per `recv` completion** over the bytes just written.
  Because a running CRC is **associative over consecutive byte runs**
  (`update(a); update(b) == update(a ++ b)`), the finalized value is
  identical whether the tail arrived in one completion or several — so
  this reproduces a single warm-cache pass in the common case and stays
  correct under a short read.

NVMe/TCP uses this to fold the DDGST CRC32C into the direct path: the
`data_digest`-on arm passes `|c| crc.update(c)`, the off arm passes the
no-op — so **no CRC work happens at all when data digests are
disabled**, matching the pre-extraction behavior exactly.

---

## 6. Safety and cancellation

`read_direct` issues a `recv` straight into a *caller's* buffer across a
crate boundary. The raw-pointer safety argument splits cleanly in two:

```text
  ┌─ owned by read_direct (it controls the await) ───────────────────┐
  │ The op is awaited INLINE, so the recv is never still in flight    │
  │ once read_direct returns. If the whole future is dropped mid-     │
  │ await, the reactor's orphan protocol holds the slab entry until   │
  │ the terminal CQE — so the kernel write target stays alive that    │
  │ long regardless of the drop.                                      │
  └───────────────────────────────────────────────────────────────────┘
  ┌─ owned by the nvme-tcp call site (it knows what dst is) ──────────┐
  │ dst..dst+len is slot-buffer memory (bounds-checked by the slicing │
  │ that produced the pointer); the slot's state is Receiving, so dst │
  │ is UNALIASED — nothing else reads or writes it; and the queue     │
  │ outlives the await via the recv task's Rc (teardown drains/leaks  │
  │ before freeing).                                                  │
  └───────────────────────────────────────────────────────────────────┘

`read_direct`'s `# Safety` therefore requires the caller to supply a
`dst` that is **valid and unaliased for `len` writes** for the duration
of the await; the left box is what the reader guarantees in return.
```

**Why no `Drop` tripwire** (unlike `StreamSender`): the sender holds ZC
notification handles *across* turns, so a drop with notifications
outstanding is a real use-after-free risk it must `debug_assert` against.
`StreamReader` holds no cross-await op handle — `read_direct` awaits its
op inline and returns before yielding, so there is never an in-flight
reader op while the reader sits idle between calls. The only
drop-during-await case is already covered by the reactor's orphan
protocol, which is the reactor's invariant, not the reader's. There is
nothing for a tripwire to assert.

The reader also **never closes `fd`**: it stores a raw `i32`, and the
connection's `OwnedFd` stays the sole owner, so the teardown contract
(the fd drops last, orphaning any in-flight op) is unchanged.

---

## 7. How NVMe/TCP wires it up

`drive_recv` (`crates/ioutgt-nvme-tcp/src/recv.rs`) constructs one
reader per connection and drives the `RecvPhase` machine out of its
window:

```rust
let mut reader = StreamReader::new(fd, 64 * 1024);
loop {
    let window = reader.fill().await?;
    if window.is_empty() { return Ok(()); }   // orderly EOF
    let window_len = window.len();
    let mut slice = window;
    while !slice.is_empty() {                  // step Header / Data / Ddgst
        // feed_header / DataPhase::advance / DdgstPhase::advance,
        // each consuming a prefix of `slice`
    }
    reader.consume(window_len);                // window fully decoded

    // large H2C write tail still to come → straight into the slot
    if let &RecvPhase::Data(data) = &phase
        && matches!(data.kind, PayloadKind::H2c { .. })
        && data.remaining >= H2C_DIRECT_MIN
    {
        phase = recv_tail_direct(queue, &mut reader, data).await?;
    }
}
```

`recv_tail_direct` computes the destination pointer into the slot and
calls `read_direct`, choosing the digest closure by `data.ddgst`:

```rust
let ptr = /* &mut slot.data()[dest .. dest + remaining]; borrow dropped before await */;
// SAFETY: see §6 — slot memory, Receiving, queue outlives the await.
let n = unsafe {
    if data.ddgst {
        reader.read_direct(ptr, data.remaining, |c| data.crc.update(c)).await?
    } else {
        reader.read_direct(ptr, data.remaining, |_| {}).await?
    }
};
if n < data.remaining { return Err(RecvEnd::Closed); } // close mid-payload
// ddgst → Ddgst phase (verify the 4 trailing bytes); else finish → Header
```

**That is the full extent of the reader's involvement.** The phase
machine, `PduDecoder`, digest construction, R2T reassembly, and dispatch
all stay in nvme-tcp. NBD reuses the same reader unchanged: a 28-byte
request header via `fill`/`consume`, and the write payload via
`read_direct` with a `|_| {}` callback.

---

## 8. Invariants

1. **Zero steady-state allocation.** The scratch buffer is allocated once
   in `new()`; `fill` and `read_direct` allocate nothing per call.
2. **No locks, no atomics.** Single thread; a plain cursor over one
   buffer.
3. **One outstanding recv, ever.** `fill` issues a recv only when the
   window is empty; `read_direct` is entered only after the window has
   drained and re-arms nothing until the tail lands. The direct tail is
   by definition the next bytes on the stream, so nothing is reordered.
4. **Slot-blind and protocol-blind.** The reader deals in byte windows
   and a caller pointer; it has no `SlotArray`, no PDU types, and no
   `ioutgt-core` dependency.
5. **The reader never closes `fd`.** The connection's `OwnedFd` is the
   sole owner; teardown is unchanged.
6. **Cancellation safety** rests on the reactor's orphan protocol (§6),
   not on reader-held state — hence no `Drop` tripwire.

---

## 9. `StreamReader` vs `StreamSender`

The two halves of a stream transport, deliberately asymmetric:

| | `StreamSender` (send) | `StreamReader` (recv) |
|---|---|---|
| Direction | push: drain work → ship | pull: read → decode → branch |
| Protocol seam | a staging **closure** the harness drives | a byte-source **object** the transport drives |
| Slot knowledge | generic over `C`/`W`; writes `SlotArray<C>` | none — byte windows + caller ptr |
| Depends on | `ioutgt-core` + `ioutgt-uring` | `ioutgt-uring` only |
| Hard part | ZC notification lifetime, double-buffer, anti-deadlock drain | raw-ptr `MSG_WAITALL` recv into a slot, cancellation safety |
| Cross-await op state | yes (ZC notifs) → `Drop` tripwire | none → no tripwire |
| Digest seam | (n/a — payload referenced in place) | `on_chunk` closure, monomorphized no-op when off |

They share the same crate, the same single-threaded no-lock discipline,
and the same goal: factor the transport-agnostic IO machinery out of
NVMe/TCP so NBD and NVMe/RDMA reuse it.

---

## In one sentence

`StreamReader` is a slot-blind, protocol-blind byte-source that owns the
recv scratch buffer and the `recv` ops, hands the transport a window to
frame headers out of (`fill`/`consume`), and receives large payloads
straight into a caller's slot buffer with a `MSG_WAITALL` short-read loop
(`read_direct`) — the recv counterpart to `StreamSender`, with all
protocol and slot specifics left to the transport.
