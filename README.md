# Event Replay System
**Building Deterministic Backends with Immutable Event Logs**

---

## Overview

Most backend systems store only the **current state** of data and overwrite previous values. This approach works in simple systems, but it **breaks down in distributed environments** where retries, partial failures, duplicate requests, and out-of-order execution are unavoidable.

When failures occur—such as duplicate payments or inconsistent state—the system can no longer explain **how it reached the current state**. Debugging becomes guesswork using logs and manual database fixes. Recovery is risky, auditing is incomplete, and confidence in correctness degrades at scale.

This project explores an alternative approach:  
**treating history as the source of truth.**

The system is built around an **append-only event log**, where all state changes are recorded as immutable facts. Current state is derived by replaying events, not by mutating records in place.

The goal is not CRUD.  
The goal is **correctness under failure**.

---

## Problem Statement

Traditional state-based systems suffer from fundamental limitations:

- Past state is lost due to overwrites
- Duplicate requests cause undefined behavior
- Retries and partial failures lead to corruption
- Root-cause analysis relies on logs instead of facts
- Recovery requires manual database fixes
- Auditing and replay are unreliable or impossible

In distributed systems, these issues are **inevitable**, not edge cases.

---

## Core Guarantees

This system is built around strict, non-negotiable guarantees:

- All facts are recorded as **immutable events** in an append-only event log
- Once written, events are **never modified or deleted**
- Event delivery to consumers is **at-least-once**; duplicates are expected
- Consumers must process events **deterministically and idempotently**
- Application state is derived through **projections** built by replaying events
- Projections are **disposable** and can be rebuilt at any time
- System recovery is performed through **replay and compensation events**, never by mutating past data

If these guarantees are violated, the system is considered broken.

---

## Architectural Philosophy

- The event log is the **single source of truth**
- State is **derived**, not authoritative
- “Exactly-once processing” is treated as a myth
- Failures are expected and designed for
- The past is immutable; mistakes are corrected by **new events**

---

## Domain: Order and Payment Workflow

The system uses a simplified **order–payment** domain to expose real distributed-system challenges:

- Duplicate API calls
- Concurrent payment attempts
- Consumer crashes
- Out-of-order event delivery
- Retry storms

The simplicity of the domain is intentional—the complexity lies in **behavior under failure**, not business rules.

---

## Event Definitions (Version 1)

The system begins with **exactly four events**:

1. `OrderCreated`
2. `PaymentRequested`
3. `PaymentSucceeded`
4. `RefundIssued`

Each event definition answers four questions:

- What fact does this event represent?
- Who is allowed to emit it?
- What must already be true?
- What must never be true?

This forces explicit reasoning about invariants.

---

### Event: `OrderCreated`

**What fact does this event represent?**  
An order has been created with a defined set of items and prices, and a unique order identifier now exists.

**Who is allowed to emit it?**  
The **Order Service**, after validating the request and persisting the event.

**What must already be true?**
- The order does not already exist
- The order contains at least one item
- Each item has a valid quantity and price
- The total amount is calculated and fixed

**What must never be true?**
- An order with the same ID already exists
- The order contains zero items
- Payment has already been marked as successful for this order

---

### Event: `PaymentRequested`

**What fact does this event represent?**  
A payment attempt has been initiated for an existing order with a fixed amount and a unique payment identifier.

**Who is allowed to emit it?**  
The **Payment Service**, after validating that the referenced order exists and is eligible for payment.

**What must already be true?**
- The referenced order exists
- The order is not already fully paid
- The payment amount matches the order total
- No other active payment attempt exists for the order

**What must never be true?**
- A successful payment already exists for the order
- Another `PaymentRequested` is active for the same order
- The order has been cancelled or closed

---

### Event: `PaymentSucceeded`

**What fact does this event represent?**  
A payment attempt has completed successfully, and the specified amount has been confirmed as received.

**Who is allowed to emit it?**  
The **Payment Service**, after receiving confirmation from the external payment provider.

**What must already be true?**
- A corresponding `PaymentRequested` exists
- The payment attempt is not already marked successful
- The referenced order exists and is payable
- The payment amount matches the requested amount

**What must never be true?**
- A successful payment already exists for this attempt
- The order has been cancelled or closed
- The payment was already failed or refunded

---

### Event: `RefundIssued`

**What fact does this event represent?**  
A refund has been successfully issued for a previously successful payment, and the specified amount has been returned or scheduled to be returned.

**Who is allowed to emit it?**  
The **Payment Service**, after confirmation from the external payment provider.

**What must already be true?**
- A corresponding `PaymentSucceeded` exists
- The payment has not already been fully refunded
- The refund amount does not exceed the original payment amount
- The order is in a refundable state

**What must never be true?**
- No successful payment exists for this refund
- A refund for the same payment and amount already exists
- The payment was never captured or was already reversed
- The refund contradicts previous refund totals

This prevents:
- Double refunds
- Negative balances
- Silent corruption

---

## What This Project Is Not

- Not a CRUD demo
- Not a happy-path system
- Not dependent on manual database fixes
- Not pretending failures do not happen

---

## What This Project Demonstrates

- Why immutable event logs enable explainability
- Why idempotency is mandatory
- How replay enables recovery and auditing
- How deterministic logic survives retries and duplication
- How real distributed systems reason about correctness

---

## Status

🚧 In active development  
Initial focus: event log design, invariants, and replay correctness

---

## Final Note

If you are looking for a project that looks impressive but avoids hard problems, this is not it.

If you want to understand **why real backend systems are built the way they are**, this project is deliberately opinionated, explicit, and strict.
