# ADR 003: Shared-responsibility idempotency via Redis atomic keys

- **Status:** Accepted
- **Date:** 2026-05-22
- **Deciders:** sagakit maintainers

## Context

The Saga pattern relies on message redelivery as its primary failure recovery
mechanism. When a worker crashes after executing a step but before
acknowledging the message, Redis Streams redelivers that message to another
worker (or the same worker on restart). The step executes again.

For steps with side effects on external systems — charging a payment provider,
sending an email, reserving inventory via a third-party API — executing twice
produces different outcomes than executing once. A double charge is not a
retriable error; it is a business failure that requires human intervention.

sagakit must provide a mechanism to detect and suppress duplicate step
executions. This ADR defines what that mechanism is, who is responsible for
which parts of it, and why.

## The core problem: libraries cannot guarantee external idempotency

A naive reading of the problem suggests the library should "just handle it."
It cannot, for a fundamental reason: sagakit does not control what happens
inside a step function. If a step calls Stripe, sends an SMS, or writes to a
database owned by another service, sagakit has no way to prevent, detect, or
undo that call after it happens.

What sagakit *can* control is:

1. Generating a stable, unique identifier for each (saga, step, attempt) tuple.
2. Providing a fast, atomic mechanism to record whether that identifier has
   already been successfully processed.
3. Surfacing that information to the step function before it executes, so the
   user's code can decide whether to proceed or skip.

This is a **shared-responsibility model**: the library provides the
infrastructure, the user provides the safe step implementation.

## Decision

sagakit implements idempotency as follows:

**1. Idempotency key construction**

For every step execution, sagakit constructs a key:

```
sagakit:idempotency:{saga_id}:{step_name}:{attempt_number}
```

- `saga_id` — unique identifier of the saga instance (UUID generated at
  saga start).
- `step_name` — the name of the step being executed, derived from the
  function name or the explicit `name` parameter on the `@step` decorator.
- `attempt_number` — incremented by sagakit on each retry of the same step.
  A redelivery of the same message uses the same `attempt_number`; a
  deliberate retry after failure increments it.

This key uniquely identifies one execution attempt of one step in one saga.
Two workers receiving the same redelivered message will compute the same key.

**2. Atomic check-and-set in Redis**

Before invoking the user's step function, sagakit executes:

```
SET sagakit:idempotency:{key} "processing" NX EX {ttl_seconds}
```

- `NX` (Set if Not eXists) makes the operation atomic. If two workers race,
  exactly one wins. The loser receives `nil` and discards the message without
  executing the step.
- `EX {ttl_seconds}` sets a TTL (default: 86400 seconds / 24 hours). Keys
  do not accumulate indefinitely; they expire after a window long enough to
  cover any realistic redelivery scenario.
- The value is updated to `"completed"` after the step function returns
  successfully, and to `"failed"` if it raises.

**3. User responsibility: writing idempotent step bodies**

sagakit prevents duplicate *sagakit-level* processing. It cannot prevent
duplicate calls to external systems if the user's step does not check the
idempotency key before acting. The `SagaContext` exposes the key:

```python
@step(compensate="refund_payment")
async def charge_payment(ctx: SagaContext, order_id: str, amount: float) -> dict:
    # User checks with their payment provider whether this key was already used
    if await payments.was_charged(ctx.idempotency_key):
        return await payments.get_charge(ctx.idempotency_key)

    return await payments.charge(
        order_id=order_id,
        amount=amount,
        idempotency_key=ctx.idempotency_key,  # passed to external system
    )
```

Many external APIs (Stripe, Braintree, Twilio) natively accept an
idempotency key parameter. For those that do not, the user must implement
their own deduplication logic (typically a database lookup keyed by
`ctx.idempotency_key`).

## Alternatives considered

### Alternative A — Library guarantees full idempotency (no user involvement)

sagakit intercepts all step executions and guarantees they never run twice,
without requiring any changes to the user's step code.

**Rejected because:**

- Technically impossible for external side effects. sagakit cannot intercept
  a call to `stripe.charge()` or `requests.post()`. Full idempotency
  requires cooperation from the external system, which is outside the
  library's control.
- Even if sagakit could intercept calls, caching return values and replaying
  them on retry assumes the external system's state has not changed between
  the original call and the retry — an assumption that is rarely safe to make.

### Alternative B — User is fully responsible (library provides nothing)

sagakit provides no idempotency primitives. Users are responsible for
implementing their own deduplication before calling sagakit.

**Rejected because:**

- Every user reinvents the same pattern, badly. The atomic check-and-set
  pattern with Redis is easy to get wrong (non-atomic read-then-write, wrong
  TTL, missing the race condition between two workers). The library should
  solve the solvable part.
- Without a library-generated key, users have no stable identifier to pass
  to external systems. They would generate their own keys with no guarantee
  of consistency across retries.

### Alternative C — Persistent database as the idempotency store

Store processed idempotency keys in a relational database (PostgreSQL, MySQL)
for stronger durability guarantees.

**Rejected because:**

- sagakit is a library, not an application. It cannot assume the user has a
  relational database, nor can it manage schema migrations on the user's
  behalf.
- Introducing a database dependency would make sagakit's infrastructure
  requirement `redis + postgres`, doubling the operational surface for a
  feature that Redis handles adequately with persistence enabled.
- Redis with AOF persistence provides sufficient durability for the TTL
  window sagakit requires. Keys do not need to survive beyond 24-72 hours;
  after that, any realistic redelivery window has passed.

### Alternative D — In-memory deduplication store (no Redis)

Track processed keys in a Python dictionary in the worker process.

**Rejected because:**

- Survives neither worker restarts nor multiple worker instances. Two workers
  sharing a Redis Streams consumer group would each have their own in-memory
  store, giving no protection against cross-worker duplicates.
- Lost on any process crash, which is exactly the failure mode idempotency
  is designed to protect against.

## Consequences

### Positive

- Atomic `SET NX` prevents race conditions between competing workers with
  no additional locking primitives.
- `ctx.idempotency_key` gives users a stable, library-generated identifier
  to pass directly to external APIs that support idempotency keys natively
  (Stripe, Twilio, and others).
- Keys expire automatically via TTL, requiring no manual cleanup.
- No new infrastructure: Redis is already required by ADR 002.

### Negative — and these are real

- **The library cannot protect users who ignore `ctx.idempotency_key`.**
  A step that calls an external API without passing the key provides no
  duplicate protection for that call. The library can warn (via logging)
  but cannot enforce correct usage.
- **TTL creates a correctness window, not a guarantee.** If a redelivery
  occurs after the TTL expires (default 24 hours), the key is gone and the
  step will execute again. This is an extreme edge case but not impossible
  in pathological failure scenarios. Users who need stronger guarantees must
  increase the TTL or implement database-backed deduplication in their step
  bodies.
- **Redis persistence must be enabled for the guarantee to hold across
  restarts.** A vanilla `docker run redis` with no persistence flags loses
  all idempotency keys on restart. Documentation must make this explicit.
- **`attempt_number` is sagakit-managed, not user-managed.** Users cannot
  arbitrarily reset attempt numbers without risking key collisions. This is
  a deliberate constraint, not an oversight.

### Neutral

- External APIs that do not support idempotency keys natively require the
  user to implement a lookup (typically a database read) before the
  side-effecting call. This is unavoidable given the shared-responsibility
  model, and is standard practice in distributed systems engineering.

## Out of scope

- Idempotency for compensation steps. Compensations are subject to the same
  redelivery risk, but their idempotency key construction follows the same
  pattern (`saga_id:compensate_{step_name}:{attempt_number}`). This is
  documented in ADR 004.
- Cross-saga deduplication. Two separate sagas with different `saga_id`
  values that happen to affect the same resource are the user's
  responsibility to coordinate.
- Idempotency key rotation or invalidation before TTL expiry.

## References

- "Idempotent Consumer" pattern: https://microservices.io/patterns/communication-style/idempotent-consumer.html
- Redis SET NX semantics: https://redis.io/commands/set/
- Stripe idempotency keys: https://stripe.com/docs/api/idempotent_requests
