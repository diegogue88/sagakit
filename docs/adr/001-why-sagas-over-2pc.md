# ADR 001: Use the Saga pattern instead of two-phase commit for distributed transactions

- **Status:** Accepted
- **Date:** 2026-05-21
- **Deciders:** sagakit maintainers

## Context

sagakit is a Python library for coordinating **multi-step operations that span
independent services or resources**, where each step has side effects and the
overall operation must either complete fully or leave the system in a consistent
recoverable state.

Concrete example:

> An order-processing flow that (1) charges a payment provider, (2) reserves
> inventory in a warehouse service, and (3) creates a shipping label with a
> carrier API. Each step talks to a different system, owned by a different team,
> exposed over HTTP, with no shared database.

This is the canonical "distributed transaction" problem. The two dominant
solutions in the industry are:

1. **Two-phase commit (2PC)** — a coordinator asks every participant to
   *prepare* (lock resources, promise to commit), then *commits* or *aborts*
   atomically once all participants vote yes.
2. **Saga pattern** — the operation is modeled as a sequence of local
   transactions; each one publishes a result, and if any fails, previously
   completed steps are undone by explicit **compensating actions**.

We must pick one as the foundational model of the library. This choice
constrains every later decision (API shape, transport, failure semantics), so
it is recorded here.

## Decision

**sagakit implements the Saga pattern. Two-phase commit is explicitly out of
scope and will not be supported, even as an opt-in mode.**

Sagas in sagakit are **orchestrated** (a central coordinator drives the steps),
not choreographed (peer-to-peer events). Orchestration is chosen because it
keeps the workflow definition in one place, which is easier to reason about,
test, and observe — properties that matter more for a small library aiming at
clarity than the looser coupling that choreography offers.

## Alternatives considered

### Alternative A — Two-phase commit (2PC)

Coordinator-driven atomic commit across heterogeneous participants, typically
implemented via XA transactions or a custom prepare/commit protocol over HTTP.

**Rejected because:**

- **Requires participant cooperation we cannot assume.** Most modern services
  (Stripe, SendGrid, internal REST APIs, S3) do not expose a `prepare` phase.
  2PC only works when every participant implements the protocol, which is
  almost never the case in the target use cases for this library.
- **Blocking locks hurt availability.** During the prepare phase, participants
  hold resources locked. If the coordinator crashes between prepare and commit,
  participants stay blocked until manual intervention or a timeout. This
  contradicts the availability assumptions of services we target.
- **Poor fit for long-running workflows.** 2PC assumes commits in seconds.
  Many real workflows (a fraud review that takes minutes, a shipping label
  that requires an external batch) cannot hold locks that long.
- **The CAP trade-off is wrong for our users.** 2PC sacrifices availability
  for strong consistency. The workflows sagakit targets — order processing,
  user onboarding, claim processing — tolerate eventual consistency and
  prioritize availability.

### Alternative B — Choreographed sagas (event-driven, no central coordinator)

Each service listens for events and publishes its own outcome; compensation is
triggered by reverse-flow events.

**Rejected (for v1) because:**

- **Hard to reason about.** The full workflow exists only implicitly across
  the event bus. New contributors cannot read one file to understand the flow.
- **Hard to test.** Verifying a workflow requires running every participant
  and replaying events; orchestrated sagas can be unit-tested as a single
  state machine.
- **Hard to observe.** No single trace ties the steps together without
  significant tracing infrastructure.

Choreography may be revisited later as an optional execution mode once the
orchestrated core is stable. It is **not** ruled out forever, only deprioritized.

### Alternative C — Try/catch with manual cleanup (no library at all)

Write the workflow as straight Python, wrap each step in `try/except`, and
issue cleanup calls in the `except` branch.

**Rejected because:**

- Fails on process crash mid-workflow — there is no durable state to resume
  from.
- No retry, no backoff, no idempotency, no dead-letter handling — each user
  reinvents the same wheel, badly.
- Compensation logic gets tangled with happy-path logic. Sagas exist
  precisely to separate the two.

This alternative is, however, the *correct* choice for workflows that fit in
a single request and tolerate full rollback by losing the work. sagakit's
README will say so explicitly.

## Consequences

### Positive

- Works with any participant that exposes a normal API — no `prepare` phase
  required.
- Tolerates long-running steps and partial failures gracefully.
- Compensation logic is explicit and co-located with the forward step, which
  improves readability and forces the author to consider failure up front.
- Each step is a local transaction, so participants retain full control of
  their own consistency.

### Negative — and these are real

- **No atomic isolation.** A saga is observable in intermediate states by
  other readers. Users must design their data model to tolerate this (for
  example, an order row that exists with status `pending_payment` before
  payment confirms). This is a real cost and the library cannot hide it.
- **Compensations are not rollbacks.** Charging a card and then "refunding"
  it is not the same as never charging it — the customer sees both
  transactions on their statement. Users must accept that compensation is a
  *semantic* undo, not a *physical* one.
- **Compensation can itself fail.** sagakit must handle this explicitly
  (retry, dead-letter, alerting) and the user must accept that some failures
  require human intervention. This is documented in ADR 004.
- **Steps must be idempotent.** Because retries are inherent to the model,
  every step and every compensation must safely tolerate being called more
  than once. The library provides idempotency keys (ADR 003), but the
  user-supplied step body must still be written defensively.

### Neutral

- Users coming from a relational-database mindset will need to adjust their
  expectations. Documentation must address this directly rather than hide it.

## Out of scope

- Distributed locking primitives (use Redis/Redlock or similar directly).
- Cross-saga transactions or nested sagas. Each saga is independent in v1.
- Strict serializability guarantees. sagakit gives **eventual** consistency
  with **compensation-based** atomicity. If you need ACID across services,
  you need a different tool.

## References

- Hector Garcia-Molina and Kenneth Salem, *Sagas*, ACM SIGMOD 1987 — the
  original paper.
- Chris Richardson, *Microservices Patterns*, chapter 4 — the modern
  canonical treatment.
- Pat Helland, *Life beyond Distributed Transactions* — on why 2PC is the
  wrong default for service-oriented systems.
