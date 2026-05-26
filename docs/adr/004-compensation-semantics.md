# ADR 004: Compensation semantics — retry, DLQ, and the non-rollback guarantee

- **Status:** Accepted
- **Date:** 2026-05-22
- **Deciders:** sagakit maintainers

## Context

When a saga step fails, sagakit executes compensating actions for all
previously completed steps in reverse order. This is the Saga pattern's
answer to atomicity: instead of a database rollback, each step declares an
explicit compensation function that semantically undoes its forward action.

This ADR addresses three questions that arise from that model:

1. What happens when a compensation function itself fails?
2. Are compensation functions subject to the same idempotency requirements
   as forward steps?
3. What contract does sagakit impose on compensation function authors —
   specifically, what assumptions can they safely make about the state of
   the system when their function executes?

These questions must be answered before implementing the compensation
execution engine, because the answers constrain the data model, the retry
logic, and the observability primitives.

## Decision

### 1. Compensation failure handling: retry then DLQ

sagakit handles a failing compensation function as follows:

**Phase 1 — Automatic retry with exponential backoff.**
sagakit retries the compensation up to a configurable maximum (default: 3
attempts) using exponential backoff with jitter. This covers transient
failures: a downstream service that is briefly unavailable, a network hiccup,
a Redis timeout. Most compensation failures in practice are transient.

**Phase 2 — Dead Letter Queue (DLQ) on exhaustion.**
If all retry attempts are exhausted, sagakit publishes the failed
compensation to a dedicated DLQ stream in Redis
(`sagakit:dlq:{saga_id}`). The message includes full context: `saga_id`,
`step_name`, the compensation function name, the original payload, the
exception chain, and a timestamp.

**Phase 3 — Structured alert.**
Alongside the DLQ write, sagakit emits a structured log event at `ERROR`
level with the same context. In production, this event is the hook for
alerting (PagerDuty, Slack, etc.) via the user's log aggregation pipeline.
sagakit does not implement alerting directly — that is the user's
infrastructure.

**Why not fail-fast (raise immediately on first compensation failure)?**
Failing fast sounds safe but leaves the system in a worse state: some
compensations ran, some did not, and there is no record of which ones failed
or why. A DLQ preserves the information needed to resume or manually
complete the compensation later. Information is more valuable than a clean
exception.

**Why not retry forever?**
Infinite retries mask permanent failures (a service that will never come
back, a business rule that makes the compensation invalid). After a
reasonable retry window, a human must be involved. The DLQ + alert is the
handoff mechanism to human operators.

### 2. Compensation functions must be idempotent

Compensation functions are subject to the same redelivery risk as forward
steps (see ADR 003). If sagakit retries a compensation, the function will
execute more than once. If it is not idempotent, the retry causes the same
class of problem it was meant to solve.

sagakit applies the same idempotency key mechanism to compensations (ADR 003),
using the key pattern:

```
sagakit:idempotency:{saga_id}:compensate_{step_name}:{attempt_number}
```

The `compensate_` prefix distinguishes compensation idempotency keys from
forward step keys, preventing collisions when a saga retries and re-executes
both a step and its compensation in the same saga instance.

The shared-responsibility model from ADR 003 applies equally here: sagakit
prevents duplicate sagakit-level processing; the user is responsible for
writing compensation bodies that are safe to call more than once.

A concrete example of an idempotent compensation:

```python
@step()
async def refund_payment(ctx: SagaContext, payment_id: str) -> dict:
    # Safe to call multiple times: check before acting
    if await payments.is_refunded(payment_id):
        return await payments.get_refund(payment_id)

    return await payments.refund(
        payment_id=payment_id,
        idempotency_key=ctx.idempotency_key,
    )
```

### 3. Compensation is semantic undo, not physical rollback

This is the most important constraint for users writing compensation
functions, and the one most commonly misunderstood by developers coming from
a relational database background.

**What compensation is not:**
A database rollback undoes a transaction as if it never happened. No
intermediate state is observable. No trace remains. The system returns to
exactly the state it was in before the transaction began.

**What compensation is:**
A compensation is a new, forward-moving action that produces a state that is
*acceptable* given that the original action happened. It does not erase
history — it adds to it.

Concrete implications for compensation authors:

- **Intermediate states are visible and real.** Between the moment a forward
  step completes and the moment its compensation runs, other systems may have
  observed and acted on the forward step's effects. A payment may have
  appeared on a bank statement. An inventory reservation may have prevented
  another order. A notification may have been sent. The compensation cannot
  un-observe those effects.

- **Compensation runs in a different world than the forward step.** The
  compensation function must not assume the system is in the same state it
  was when the forward step ran. Time has passed. Other sagas may have run.
  Downstream services may have changed state in response to the forward step.
  A compensation that assumes "I am undoing X" when it should assume "I am
  applying a corrective action given that X already happened" will be
  brittle.

- **The user experience of compensation is different from the user experience
  of never having run the step.** A customer who is charged and then refunded
  sees two transactions on their statement, not zero. A warehouse that
  receives a reservation and then a cancellation may have allocated physical
  space in the interim. sagakit cannot hide this from users of the system
  being orchestrated. Documentation and user-facing messaging must account
  for it.

## Alternatives considered

### Alternative A — Fail fast: raise immediately on compensation failure

If a compensation fails, surface the exception immediately and stop all
further compensation attempts.

**Rejected because:**

- Leaves the saga in an indeterminate state with no record of which
  compensations succeeded and which failed.
- Loses the context needed for manual recovery (payload, exception, step
  name, attempt history).
- Transfers all recovery burden to the operator, who now must reconstruct
  what happened from application logs rather than from a structured DLQ
  message.

### Alternative B — Retry forever until compensation succeeds

Retry the failing compensation indefinitely, on the assumption that all
failures are transient and will eventually resolve.

**Rejected because:**

- Masks permanent failures. If the inventory service is decommissioned, no
  amount of retrying will succeed. Infinite retries burn resources and delay
  the operator alert that is actually needed.
- Makes saga completion time unbounded and unpredictable.
- Provides no structured output for observability tooling.

### Alternative C — Skip failed compensations and continue with the rest

If a compensation fails after retries, log it and continue compensating the
remaining steps.

**Partially adopted:** sagakit does continue compensating remaining steps
after sending a failed compensation to the DLQ. Stopping all compensation
because one step's compensator is broken would leave more of the system in
an uncompensated state. However, "skip" without a DLQ write is rejected —
the failure must be recorded and alerted.

### Alternative D — Compensations are not required to be idempotent

Treat compensation functions as one-shot: if they fail, retry the entire
saga from the beginning rather than retrying the compensation.

**Rejected because:**

- Restarting the entire saga re-executes forward steps that already
  succeeded, multiplying side effects rather than reducing them.
- The redelivery risk for compensations is identical to the redelivery risk
  for forward steps. Ignoring it for compensations while addressing it for
  forward steps is an inconsistent and indefensible design.

## Consequences

### Positive

- DLQ provides a structured, queryable record of all compensation failures,
  enabling automated or manual recovery workflows.
- Retry with backoff handles the majority of transient failures without
  operator involvement.
- Consistent idempotency model across forward steps and compensations
  reduces the cognitive surface area the user must reason about.
- Explicit documentation of the non-rollback guarantee sets correct
  expectations for users and prevents a class of design mistakes.

### Negative — and these are real

- **Some sagas will require human intervention to resolve.** This is not a
  library failure — it is an inherent property of distributed systems without
  2PC. sagakit makes it visible and manageable rather than pretending it
  cannot happen.
- **Users must write more defensive compensation code than they might
  expect.** Checking before acting (e.g., `if already_refunded: return`)
  adds lines to what developers often assume will be simple cleanup logic.
  The library cannot enforce this discipline — only document it clearly.
- **DLQ messages require operational tooling to be useful.** A DLQ entry in
  Redis is only actionable if someone (or something) is monitoring it.
  sagakit provides the structured log event as the alerting hook, but the
  user must wire it to their alerting infrastructure.

### Neutral

- Users familiar with message queue DLQ patterns (SQS, Service Bus, RabbitMQ)
  will find sagakit's DLQ semantics familiar. Users coming from synchronous
  backgrounds may need to adjust their mental model of what "failure" means
  in an eventually-consistent system.

## Out of scope

- Automatic saga state reconstruction from DLQ messages. Replaying a failed
  compensation from the DLQ is a manual or user-implemented operation in v1.
- Compensation ordering guarantees beyond reverse-step-order. sagakit
  compensates in strict reverse order of successful forward steps. Partial
  ordering or parallel compensation is not supported in v1.
- Cross-saga compensation coordination. If two sagas share a resource and
  both need to compensate against it simultaneously, the user is responsible
  for coordinating access.

## References

- Garcia-Molina and Salem, *Sagas*, ACM SIGMOD 1987 — original treatment of
  compensation semantics.
- Pat Helland, *Memories, Guesses, and Apologies* — on the nature of
  compensation in distributed systems.
- Dead Letter Queue pattern: https://microservices.io/patterns/communication-style/messaging.html
