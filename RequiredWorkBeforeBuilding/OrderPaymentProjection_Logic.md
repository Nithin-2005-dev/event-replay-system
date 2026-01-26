# Event Replay Based Payment System

## Overview

This project demonstrates a **real-world event-sourced payment system** built around a single core idea:

> **The event log is the only source of truth.  
> All system state is derived by replaying events.**

Instead of storing and mutating the current state directly, the system records **immutable events** and derives the current payment state through deterministic replay.

This approach is designed to handle:
- retries
- duplicate requests
- partial failures
- concurrent systems
- financial correctness
- full auditability

---

## Problem Statement

Traditional backend systems store only the *current state* and overwrite previous values.

In distributed systems, this breaks down because:
- requests can be retried
- consumers can crash
- events can be duplicated
- operations are not atomic across services

When something goes wrong (e.g., double payments, incorrect refunds), the system:
- cannot explain how it reached the current state
- requires manual DB fixes
- loses confidence and auditability

This project solves that by **never mutating history**.

---

## Core Guarantees

The system is built on the following guarantees:

- All facts are recorded as **immutable events**
- Events are appended only (no updates, no deletes)
- Event delivery is **at-least-once**
- Duplicate events are expected and valid
- Consumers must be **idempotent**
- Application state is derived via **replay**
- Projections are **disposable**
- Recovery happens via **replay**, not DB mutation

---

## Event Model

### Event Log (`event_log`)

The `event_log` table is the **single source of truth**.

Each row represents a fact that happened in the system.

Typical fields:
- `event_id`
- `event_sequence` (monotonic ordering)
- `aggregate_id` (order id)
- `event_type`
- `payload` (JSON)
- `created_at`

Events are **never modified or deleted**.

---

## Supported Events (V1)

### 1. OrderCreated

**Fact:**  
An order with items and total amount now exists.

**Rules:**
- Order must not already exist
- Must contain at least one item
- Total amount is fixed

---

### 2. PaymentRequested

**Fact:**  
A payment attempt was initiated for an order.

**Rules:**
- Order must exist
- Order must not already be fully paid
- Only one active payment request allowed

---

### 3. PaymentSucceeded

**Fact:**  
Money has been successfully captured.

**Rules:**
- A prior PaymentRequested must exist
- Amount must match the request
- Represents irreversible financial inflow

---

### 4. RefundIssued

**Fact:**  
Money has been returned (or scheduled to be returned).

**Rules:**
- A successful payment must exist
- Refund amount must not exceed paid amount
- Prevents double refunds and negative money

---

## Projection Model

### `order_payment_projection`

This table represents the **derived current state** of each order.

It is:
- fully disposable
- rebuilt from scratch on replay
- never used as a source of truth

Fields:
- `aggregate_id`
- `payment_state`
- `order_total_amount`
- `paid_amount`
- `refunded_amount`
- `is_settled`
- `refund_allowed`

---

## Replay Strategy

Replay is performed in two phases:

---

## Phase 1 – Validation (Pre-flight Checks)

Before replaying, the system validates that the event history is **logically possible**.

If any rule fails, replay is aborted.

### Validation Rules

1. No duplicate `OrderCreated`
2. `PaymentSucceeded` must have a prior `PaymentRequested`
3. `PaymentRequested` must not occur after `PaymentSucceeded`
4. `RefundIssued` must have a prior `PaymentSucceeded`

Validation protects replay from **impossible timelines**.

---

## Phase 2 – Replay (Deterministic Rebuild)

Replay always follows this pattern:

1. **TRUNCATE projection**
2. **Recompute state from event log**
3. **Never read projection values for computation**

---

### Step A – OrderCreated

Initializes projection rows for all orders.

Establishes:
- aggregate identity
- order total
- base state

---

### Step B – PaymentRequested

Marks orders where a payment intent exists.

Important:
- No money is touched
- Based on existence, not count
- Duplicate requests are harmless

---

### Step C – PaymentSucceeded

Derives total money received.

Key rule:
- `paid_amount = SUM(PaymentSucceeded.amount)`
- No incremental updates
- Projection state is never an input

This guarantees replay safety and financial correctness.

---

### Step D – RefundIssued

Derives total money refunded.

Key rule:
- `refunded_amount = SUM(RefundIssued.amount)`
- State is **derived**, not mutated

Derived fields:
- `payment_state`
- `is_settled`
- `refund_allowed`

Refund logic is intentionally strict to prevent money loss.

---

## Key Design Principles

### Full Replay vs Streaming

- **Full replay** → total recomputation
- **Streaming consumers** → incremental mutation

This project focuses on **full replay correctness first**.

---

### Golden Rules

- Event log = truth
- Projection = opinion
- Replay recomputes, never mutates
- Money math is derived, never accumulated
- Validation enforces timeline correctness

---

## Why This Design Works

- Deterministic under unlimited replays
- Resistant to duplicates and retries
- Auditable and explainable
- Safe for financial systems
- Matches real-world distributed system behavior

---

## Future Extensions

- Incremental consumers with offsets
- Idempotency keys
- Kafka-style streaming
- Concurrent payment attempts
- Partial captures
- Multi-currency support

---

## Summary

This project demonstrates **event sourcing done correctly**:
- immutable history
- strict invariants
- deterministic replay
- financially safe projections

It is intentionally designed to reflect **real production constraints**, not CRUD simplicity.
