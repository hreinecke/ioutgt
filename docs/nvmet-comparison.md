# ioutgt vs Linux kernel nvmet — design comparison

Status: skeleton; each section is completed when the corresponding ioutgt
subsystem lands (final pass at M10). Format per subsystem: Linux design /
ioutgt design / differences / benefits / risks.

Reference sources: `drivers/nvme/target/{core.c,tcp.c,fabrics-cmd.c,
admin-cmd.c,discovery.c,io-cmd-bdev.c,io-cmd-file.c,nvmet.h}`.

## 1. Command lifecycle and request tracking
- Linux: `nvmet_req` embedded in transport command; `percpu_ref` per SQ;
  sqhd via cmpxchg; lock-free llist for responses.
- ioutgt: preallocated `CmdSlot` array, persistent task per tag,
  single-threaded queue ownership (no atomics at all). *(details at M3)*

## 2. TCP transport
- Linux: softirq → `io_work` on bound CPU, budgeted recv/send loops,
  kernel_sendpage/recvmsg.
- ioutgt: io_uring reactor on a pinned thread, identical PDU state
  machines, batched ring submission. *(details at M5)*

## 3. Fabrics connect / controller model
*(at M4)*

## 4. Admin command surface
*(at M4)*

## 5. Discovery
*(at M4)*

## 6. IO backends
- Linux: bio submission (`io-cmd-bdev.c`) / kiocb + workqueue
  (`io-cmd-file.c`).
- ioutgt: ring READ/WRITE on the queue thread, O_DIRECT. *(details at M6)*

## 7. Threading and locking
*(at M5/M9, with measurements)*

## 8. What ioutgt deliberately does differently
- Task-per-tag async instead of state-machine callbacks.
- No CID lookup structure (TTAG = slot index).
- Userspace: no softirq sharing, explicit core ownership.
*(expanded at M10 with benchmark evidence)*
