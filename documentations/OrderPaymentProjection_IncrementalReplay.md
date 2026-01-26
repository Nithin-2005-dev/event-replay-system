---
document_metadata:
  title: "Beyond Full Replay: Why Incremental Replay Is Needed"
  author: "Architect"
  version: "1.0"
core_argument:
  problem: "Full replay is mathematically correct but operationally expensive."
  constraints:
    - "Event logs grow large"
    - "Consumers must react in near real-time"
    - "Rebuilding projections from scratch on every change is too slow"
architectural_principles:
  optimization: "Incremental Replay"
  guarantees:
    full_replay: "Correctness"
    incremental_replay: "Efficiency"
  constraint: "Incremental replay must never violate the guarantees established by full replay."
---

# Beyond Full Replay: Why Incremental Replay Is Needed

Full replay guarantees correctness, but it is not always operationally feasible. 

In real systems:
- Event logs grow large.
- Consumers must react in near real-time.
- Rebuilding projections from scratch on every change is too slow.

Therefore, systems introduce **incremental replay** as an optimization — *not as a replacement for full replay*.

> **Full replay guarantees correctness.** > **Incremental replay guarantees efficiency.**

Incremental replay must never violate the guarantees established by full replay.

---

## Consumers

A **consumer** is an independent unit of logic that:
- Reads events from the event log.
- Derives its own projection or side effect.

**Examples:**
- Payment projection consumer
- Fraud detection consumer
- Notification consumer
- Analytics consumer

### Key Rule

> Consumers are **independent** and **isolated**.  
> They share the event log, but nothing else.

Each consumer:
- Progresses at its own speed.
- Can crash independently.
- Can be rebuilt independently.
- Owns its own offset.

---

## Consumer Offsets

Offsets represent **consumer progress**, not system truth. An offset answers one question:

> “Up to which event has *this consumer* safely applied its logic?”

### Properties
- Offsets are **per consumer**.
- Offsets are **monotonic**.
- Offsets are updated **only after safe application**.
- Offsets are meaningless outside the consumer that owns them.

### Why offsets are not enough
Offsets alone assume: *“Everything before this offset was applied exactly once.”* This assumption breaks under crashes, retries, and partial transactions. Therefore, offsets **must be combined with idempotency tracking**.

---

## Processed Events (Idempotency)

Incremental replay operates under **at-least-once delivery**. This means the same event can be seen multiple times, retries are expected, and duplicates are normal. To remain correct, consumers must be **idempotent**. 

This is achieved using a `processed_events` table.

### Purpose of `processed_events`
It answers: *“Has this specific consumer already applied this specific event?”* This enables:
- Safe retries
- Duplicate detection
- Crash recovery
- Deferred processing



---

## Atomic Processing Model

For incremental replay to be correct, the following operations **must be atomic**:

1. Validate event against invariants.
2. Apply projection updates.
3. Mark event as processed.
4. Advance consumer offset.

These steps must occur **inside a single database transaction**.

### Why this matters
If a crash happens:
- **Before commit** → Nothing happened, safe retry.
- **After commit** → Everything happened, safe skip.

There is no intermediate state.



---

## Offset Advancement Rule (Critical)

The consumer offset must be advanced **only after**:
- The event has been successfully applied to the projection.
- The event has been persisted as processed.

Advancing the offset earlier causes data loss, double application, or silent corruption. This rule is non-negotiable.

---

## Incremental Replay Flow

1. **Read** next event after offset.
2. **Check** `processed_events`. If already processed → skip.
3. **Validate** timeline invariants. If unmet → defer or fail.
4. **Apply** projection update.
5. **Record** event as processed.
6. **Advance** offset.
7. **Commit** transaction.

---

## Deferred Events (Waiting State)

Some events may be **valid facts** but **not yet applicable**. 

**Examples:**
- `PaymentSucceeded` arriving before `PaymentRequested`.
- `RefundIssued` arriving before `PaymentSucceeded`.

Such events must not be discarded and must not corrupt projections. They are recorded as received, marked as deferred, and retried when prerequisites are satisfied. This behavior requires processed-event tracking; it cannot be modeled using offsets alone.

---

## Full Replay vs Incremental Replay

| Aspect | Full Replay | Incremental Replay |
| :--- | :--- | :--- |
| **Purpose** | Correctness | Performance |
| **Trusts offsets** | ❌ No | ✅ Yes |
| **Uses processed_events** | ❌ No | ✅ Yes |
| **Reads entire log** | ✅ Yes | ❌ No |
| **Handles corruption** | ✅ Yes | ❌ No |
| **Disposable projections**| ✅ Yes | ⚠️ No |

> **Incremental replay is an optimization. Full replay is the safety net.**

---

## When Incremental Replay Must Be Abandoned

A consumer must force a **full replay** when:
- Invariants fail during incremental processing.
- Timeline assumptions break.
- Projection enters an impossible state.
- Consumer logic changes.
- Offset and processed-events disagree.

In these cases, all consumer-derived state is discarded, projections are rebuilt from scratch, and offsets are reset.

---

## Mental Model Summary

- **Event log** → Truth
- **Projection** → Derived opinion
- **Offsets** → Consumer progress
- **processed_events** → Idempotency guarantee
- **Full replay** → Correctness
- **Incremental replay** → Efficiency

Anything derived can be deleted. Only the event log is sacred.

---

## Status of This Project

- Full replay logic is fully designed.
- Invariants are formally defined.
- Consumer model is finalized.

The next phase is **mechanical implementation**: SQL schemas for offsets and processed events, incremental replay SQL, and Spring Boot consumer wiring.

Correctness comes first. Code comes next.
