---

## Beyond Full Replay: Why Incremental Replay Is Needed

Full replay guarantees **correctness**, but it is not always operationally feasible.

In real systems:
- event logs grow large
- consumers must react in near real-time
- rebuilding projections from scratch on every change is too slow

Therefore, systems introduce **incremental replay** as an optimization — *not as a replacement for full replay*.

> **Full replay guarantees correctness.  
> Incremental replay guarantees efficiency.**

Incremental replay must never violate the guarantees established by full replay.

---

## Consumers

A **consumer** is an independent unit of logic that:
- reads events from the event log
- derives its own projection or side effect

Examples:
- payment projection consumer
- fraud detection consumer
- notification consumer
- analytics consumer

### Key rule

> Consumers are **independent** and **isolated**.  
> They share the event log, but nothing else.

Each consumer:
- progresses at its own speed
- can crash independently
- can be rebuilt independently
- owns its own offset

---

## Consumer Offsets

Offsets represent **consumer progress**, not system truth.

An offset answers one question:

> “Up to which event has *this consumer* safely applied its logic?”

### Properties

- Offsets are **per consumer**
- Offsets are **monotonic**
- Offsets are updated **only after safe application**
- Offsets are meaningless outside the consumer that owns them

### Why offsets are not enough

Offsets alone assume:
> “Everything before this offset was applied exactly once”

This assumption breaks under:
- crashes
- retries
- partial transactions

Therefore offsets **must be combined with idempotency tracking**.

---

## Processed Events (Idempotency)

Incremental replay operates under **at-least-once delivery**.

This means:
- the same event can be seen multiple times
- retries are expected
- duplicates are normal

To remain correct, consumers must be **idempotent**.

This is achieved using a `processed_events` table.

### Purpose of `processed_events`

It answers:

> “Has *this specific consumer* already applied *this specific event*?”

This enables:
- safe retries
- duplicate detection
- crash recovery
- deferred processing

---

## Atomic Processing Model

For incremental replay to be correct, the following operations **must be atomic**:

1. Validate event against invariants
2. Apply projection updates
3. Mark event as processed
4. Advance consumer offset

These steps must occur **inside a single database transaction**.

### Why this matters

If a crash happens:
- before commit → nothing happened, safe retry
- after commit → everything happened, safe skip

There is no intermediate state.

---

## Offset Advancement Rule (Critical)

The consumer offset must be advanced **only after**:

- the event has been successfully applied to the projection
- the event has been persisted as processed

Advancing the offset earlier causes:
- data loss
- double application
- silent corruption

This rule is non-negotiable.

---

## Incremental Replay Flow

At a high level:

1. Read next event **after offset**
2. Check `processed_events`
    - if already processed → skip
3. Validate timeline invariants
    - if unmet → defer or fail
4. Apply projection update
5. Record event as processed
6. Advance offset
7. Commit transaction

This flow guarantees correctness under:
- retries
- duplicates
- crashes

---

## Deferred Events (Waiting State)

Some events may be **valid facts** but **not yet applicable**.

Examples:
- `PaymentSucceeded` before `PaymentRequested`
- `RefundIssued` before `PaymentSucceeded`

Such events:
- must not be discarded
- must not corrupt projections

They are:
- recorded as received
- marked as deferred
- retried when prerequisites are satisfied

This behavior cannot be modeled using offsets alone — it requires processed-event tracking.

---

## Full Replay vs Incremental Replay

| Aspect | Full Replay | Incremental Replay |
|------|------------|-------------------|
| Purpose | Correctness | Performance |
| Trusts offsets | ❌ No | ✅ Yes |
| Uses processed_events | ❌ No | ✅ Yes |
| Reads entire log | ✅ Yes | ❌ No |
| Handles corruption | ✅ Yes | ❌ No |
| Disposable projections | ✅ Yes | ⚠️ No |

### Key rule

> **Incremental replay is an optimization.  
> Full replay is the safety net.**

---

## When Incremental Replay Must Be Abandoned

A consumer must force a **full replay** when:

- invariants fail during incremental processing
- timeline assumptions break
- projection enters an impossible state
- consumer logic changes
- offset and processed-events disagree

In these cases:
- all consumer-derived state is discarded
- projections are rebuilt from scratch
- offsets are reset after successful replay

---

## Mental Model Summary

- Event log → **truth**
- Projection → **derived opinion**
- Offsets → **consumer progress**
- processed_events → **idempotency guarantee**
- Full replay → **correctness**
- Incremental replay → **efficiency**

Anything derived can be deleted.  
Only the event log is sacred.

---

## Why This Matters

This architecture:
- survives crashes
- tolerates duplicates
- prevents money corruption
- enables audits
- supports evolution
- matches real distributed systems

It trades simplicity for correctness — intentionally.

---

## Status of This Project

At this stage:
- full replay logic is fully designed
- invariants are formally defined
- consumer model is finalized

The next phase is **mechanical implementation**:
- tables for offsets and processed events
- incremental replay SQL
- Spring Boot consumer wiring

Correctness comes first. Code comes next.

---
