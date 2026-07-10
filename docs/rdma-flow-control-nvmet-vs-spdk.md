# NVMe-oF/RDMA target flow control: nvmet-rdma vs SPDK (exact mechanisms)

Read from source — Linux `v7.1-rc7` `drivers/nvme/target/rdma.c`, SPDK `v23.05`
`lib/nvmf/rdma.c`. This is the *reference behavior* ioutgt-nvme-rdma should be measured
against. See also `docs/rdma-64k-congestion-wedge.md`.

## TL;DR — the shared model

Both targets implement the **same kind** of flow control:

1. A **credit/budget** on outstanding RDMA work, **sized to the NVMe queue depth**.
2. When a command would exceed the budget, it is **parked on a FIFO wait queue, not dropped**,
   and **retried when a completion frees a credit**.
3. **No congestion reaction** — no CNP handling, no rate limiting, no window shrink on loss.
   Both **assume a lossless fabric** (PFC/DCQCN configured in the network).

The only real difference is *how many* budgets and how the data path is built. Because the
window equals the queue depth in both, **both offer queue-depth-worth of RDMA to the wire**, so
neither prevents a fabric-congestion wedge in software (which is why nvmet wedges on our box too).

---

## nvmet-rdma — one send-WR credit, one wait list

**Sizing** (`nvmet_rdma_create_queue_ib`, rdma.c:1262-1314):
- `qp_attr.cap.max_send_wr = queue->send_queue_size + 1;`  *(+1 for drain)* — :1289
- `qp_attr.cap.max_rdma_ctxs = queue->send_queue_size * rdma_rw_mr_factor(dev, port, 1<<NVMET_RDMA_MAX_MDTS);` — :1290-1292 (separate rdma_rw context pool for the READ/WRITE WRs)
- CQ: `nr_cqe = recv_queue_size + 2*send_queue_size` (+1) — :1271
- `atomic_set(&queue->sq_wr_avail, qp_attr.cap.max_send_wr);` — :1314

**The credit** = `atomic_t sq_wr_avail` (struct field rdma.c:94), initialized to `send_queue_size + 1`.

**Reserve** (`nvmet_rdma_execute_command`, rdma.c:942-964):
```c
if (unlikely(atomic_sub_return(1 + rsp->n_rdma, &queue->sq_wr_avail) < 0)) {
        atomic_add(1 + rsp->n_rdma, &queue->sq_wr_avail);   // back it out
        return false;                                       // send queue full
}
```
`1 + n_rdma` = **1 SEND (the NVMe response) + n_rdma RDMA READ/WRITE WRs**; `n_rdma` comes from
`rdma_rw_ctx_init` (:638, `rsp->n_rdma += ret;` :896).

**Defer, don't drop** (`nvmet_rdma_handle_command`, rdma.c:985-988):
```c
if (unlikely(!nvmet_rdma_execute_command(cmd))) {
        list_add_tail(&cmd->wait_list, &queue->rsp_wr_wait_list);   // park
}
```

**Release + drain** (`nvmet_rdma_release_rsp`, rdma.c:664-673):
```c
atomic_add(1 + rsp->n_rdma, &queue->sq_wr_avail);        // free credits
...
if (!list_empty_careful(&queue->rsp_wr_wait_list))
        nvmet_rdma_process_wr_wait_list(queue);          // re-run parked cmds (:513-530)
```

**Data path**: `rdma_rw` (the kernel RDMA R/W API) — `rdma_rw_ctx_init` (:638) / `rdma_rw_ctx_post`
(:956). rdma_rw internally chunks a large transfer into `n_rdma` WRs; the target reserves all of
them up front against `sq_wr_avail`. **RECV**: shared SRQ (`nvmet_rdma_srq_size = 1024`, min 256) or
per-queue (`max_recv_wr = 1 + recv_queue_size`, :1300); one RECV re-posted per completion. **rsp
pool**: `2 * recv_queue_size` preallocated (`nvmet_rdma_alloc_rsps`), auto-expands with `kzalloc`
on exhaustion.

**Net:** one credit bounds `Σ(1 + n_rdma) ≤ send_queue_size + 1` across all in-flight commands.
No congestion logic.

---

## SPDK — three budgets, three FIFO queues, a request state machine

**Sizing** (`nvmf_rdma_qpair_initialize`, rdma.c:1032; `nvmf_rdma_event_accept`/depth calc :1286-1327):
- `rqpair->max_send_depth = spdk_min(max_queue_depth * 2, qp_init_attr.cap.max_send_wr);` — :1032
- `max_read_depth = min(max_queue_depth, port->device->attr.max_qp_init_rd_atom, rdma_param->initiator_depth);`
  → `rqpair->max_read_depth` — :1286-1327 (this is the RDMA-READ / IRD-ORD depth)
- live counters `current_send_depth`, `current_read_depth` (struct fields :324-329)

**The state machine** (`nvmf_rdma_request_process`, body at rdma.c:2006; decl :532) advances each request through
`RDMA_REQUEST_STATE_*` (:60-102). Flow control lives in three "PENDING" gates:

1. **iobuf pool (data buffers)** — `RDMA_REQUEST_STATE_NEED_BUFFER`:
   `STAILQ_INSERT_TAIL(&rgroup->group.pending_buf_queue, &rdma_req->req, buf_link);` (:2092-2093);
   `spdk_nvmf_request_get_buffers(...)` (:1615) parks until buffers free. **Bounds in-flight DATA.**

2. **RDMA READ (host→ctrlr, write-cmd data)** — `DATA_TRANSFER_TO_CONTROLLER_PENDING` (:2134):
   head-of-`pending_rdma_read_queue` only (:2138-2141), then
   ```c
   if (current_send_depth + num_outstanding_data_wr > max_send_depth
    || current_read_depth + num_outstanding_data_wr > max_read_depth) {
           break;   // stay pending — "we can only have so many WRs outstanding"
   }
   ```
   (:2142-2147)

3. **RDMA WRITE (ctrlr→host, read-resp data)** — `DATA_TRANSFER_TO_HOST_PENDING` (:2286):
   head-of-`pending_rdma_write_queue` only (:2290), then
   ```c
   if ((current_send_depth + num_outstanding_data_wr + 1) > max_send_depth) {
           break;   // +1 for the response SEND
   }
   ```
   (:2294-2300)

**Charge/release**: `current_read_depth += n; current_send_depth += n` (:1118-1119),
`current_send_depth += n + 1` (:1183); on completion `current_send_depth--` / `current_read_depth--`
/ `num_outstanding_data_wr--` (:4380-4392, :4541-4542, :4589-4595), which lets the pending queues drain.

**Queue scope:** `pending_rdma_read_queue` / `pending_rdma_write_queue` are **per-qpair**
(`rqpair->`, :2127/:2281); `pending_buf_queue` is **poll-group-scoped** (`rgroup->group.`, :2093),
i.e. shared across all qpairs in the poll group. So the iobuf budget is a *global* backpressure,
the two WR budgets are per-connection.

**Net:** three independent credits (iobuf pool, read-depth = qd, send-depth = 2×qd), three FIFO
park queues, defer-don't-drop. RECV via per-qpair recvs or an optional shared SRQ
(`rqpair->srq`/`poller->srq`, :1020/:1039). No congestion logic.

---

## Side by side

| | nvmet-rdma | SPDK |
|---|---|---|
| Budgets | **1** — `sq_wr_avail` (send-WR credit) | **3** — iobuf pool, `max_read_depth`, `max_send_depth` |
| Window | `send_queue_size` (= queue depth) | read=qd, send=2×qd, data=iobuf pool |
| Park queue | 1 — `rsp_wr_wait_list` | 3 FIFO — `pending_buf_queue`, `pending_rdma_read_queue`, `pending_rdma_write_queue` |
| On "full" | defer (list) + retry on completion | stay in PENDING state + retry on completion |
| Data path | `rdma_rw` (kernel R/W API, chunks internally) | manual WRs built from the iobuf pool |
| RECV | SRQ (1024) or per-queue | per-qpair recvs, or optional SRQ |
| Congestion / CNP | **none** — lossless-fabric assumption | **none** — lossless-fabric assumption |
| Drop on overrun (WR budget) | never — park † | never — park |

† Orthogonal OOM edge case in nvmet only: if the rsp pool sbitmap is exhausted **and** the
`kzalloc_obj` fallback fails, `nvmet_rdma_recv_done`/`get_rsp` does `post_recv(); return;` —
*"silently drop and have the host retry as we can't even fail it"* (rdma.c ~:230). Separate from the
WR-budget flow control analyzed here, which is strictly park-not-drop.

**Common denominator:** credit-based backpressure, **bounded to the queue depth**, park-not-drop,
and **zero fabric-congestion awareness** — both delegate losslessness to PFC/DCQCN.

---

## Where ioutgt-nvme-rdma stands vs these

- **Has (partial):** the `BufPool` is the analog of SPDK's iobuf gate — `lease_await` parks a read
  command when the pool is exhausted (def `slotq.rs:166`, called from the shared read path
  `ioutgt-nvme/src/io.rs:132`). But only the **read** path awaits; the **write-data-pull** path uses
  `lease_or_owned` (never-block, private-buffer fallback; def `slotq.rs:176`, called from
  `ioutgt-nvme-rdma/src/target.rs:678`), so it does *not* backpressure.
- **Lacks:** a **send-WR credit** (nvmet's `sq_wr_avail`) and a **read-depth counter** (SPDK's
  `current_read_depth`/`max_read_depth`). ioutgt posts WRs directly (`post_reads_batch` /
  `post_responses_batch` → `g.post()`); a send-queue overflow is treated as **fatal** (tears the
  queue down), not deferred. There is no park-and-retry queue for WR budget.
- **Consequence:** ioutgt is *less* defensive than both references on the WR-budget axis. Aligning
  means adding a send-WR/read-depth credit with defer-on-full (matching nvmet/SPDK), sized to the
  queue depth for parity — and, if we want to *prevent* the congestion wedge rather than just match
  the references, sizing that credit **below** the queue depth (which neither reference does — they
  rely on the fabric). See `docs/rdma-64k-congestion-wedge.md` §7.
