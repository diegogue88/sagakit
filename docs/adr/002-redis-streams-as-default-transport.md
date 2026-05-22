# ADR 002: Use Redis Streams as the default (and only v1) transport

- **Status:** Accepted
- **Date:** 2026-05-22
- **Deciders:** sagakit maintainers

## Context

sagakit needs a message transport layer to move saga step events between the
orchestrator and the workers that execute each step. The transport must:

- Deliver messages durably (survive a worker crash mid-saga)
- Support consumer groups (multiple workers competing for the same queue)
- Allow the orchestrator to track which steps completed and which failed
- Be practical to run locally for development and testing

This decision affects which infrastructure a developer must provision to use
sagakit, which directly determines how easy it is to adopt the library. For a
library that targets individual engineers and small teams — not enterprise
platform teams — the operational cost of the transport matters as much as its
technical capabilities.

The transport interface is designed as an ABC (`Transport`) from day one so
that additional implementations can be added in future versions without
breaking the public API. This ADR governs only the choice of the default
implementation shipped in v1.

## Decision

**sagakit v1 ships with Redis Streams as its only transport implementation.**
RabbitMQ, Kafka, and cloud-managed brokers (Azure Service Bus, AWS SQS,
Google Pub/Sub) are explicitly out of scope for v1, though the `Transport`
ABC leaves the door open for community-contributed adapters.

## Alternatives considered

### Alternative A — Apache Kafka

Kafka is the dominant event streaming platform in the industry. It offers
high throughput, durable log storage, consumer groups, and replay semantics
that map well to saga execution.

**Rejected because:**

- **Operationally heavy for the target use case.** A minimal local Kafka
  setup requires Kafka itself plus ZooKeeper (or KRaft in newer versions),
  and optionally a Schema Registry. A developer trying sagakit for the first
  time should not need a multi-container stack before writing their first
  saga. The onboarding friction directly contradicts the library's goal of
  being approachable.
- **Sized for big data, not for saga orchestration.** Kafka's throughput
  and retention guarantees are designed for millions of events per second
  across massive pipelines. sagakit's workload — saga state transitions for
  business workflows — is orders of magnitude smaller. Using Kafka here is
  choosing a freight train to commute to work.
- **Adds a Kafka client dependency.** `confluent-kafka` or `aiokafka` are
  not trivial dependencies and introduce their own operational complexity
  (Kafka version compatibility, consumer group offset management). sagakit
  aims for a minimal dependency footprint.

### Alternative B — RabbitMQ

RabbitMQ is a mature, battle-tested message broker widely used in enterprise
systems. It has good Python support (`aio-pika`) and native support for
competing consumers and acknowledgements.

**Rejected because:**

- **Still requires a separate broker process with non-trivial setup.**
  While lighter than Kafka, RabbitMQ still needs its own container, port
  exposure, and management UI to be usable. A `docker run` for RabbitMQ
  includes management plugins and has a noticeably larger footprint than
  Redis.
- **Less ubiquitous in the target developer's stack.** Most Python backend
  developers already have Redis in their stack (for caching, rate limiting,
  or session storage). RabbitMQ is far less common as a default
  infrastructure component, which means adding it has a higher chance of
  being "net new" infrastructure for the user.
- **AMQP protocol complexity.** RabbitMQ's exchange/queue/binding model,
  while powerful, adds conceptual overhead that is unnecessary for
  sagakit's use case. Redis Streams' consumer group model maps more
  directly to what the library needs.

### Alternative C — Azure Service Bus (or other cloud-managed brokers)

Azure Service Bus, AWS SQS/SNS, and Google Pub/Sub are fully managed
messaging services that eliminate operational burden in production
environments. sagakit's author has direct production experience with Azure
Service Bus.

**Rejected because:**

- **Requires cloud credentials and a paid account to run locally.** A
  developer cannot test their saga logic offline or in CI without either
  paying for a cloud subscription or running a local emulator (Azurite for
  Service Bus, which has known limitations). This is an unacceptable barrier
  for a library that aims to be easy to evaluate.
- **Locks the library into a vendor ecosystem.** sagakit is a
  cloud-agnostic open source library. Shipping Azure Service Bus as the
  default transport would signal to AWS and GCP users that the library is
  not for them. Neutrality matters for adoption.
- **Production experience with a service does not make it the right
  default.** Author familiarity is not a technical justification. The
  decision must serve the library's users, not the author's existing
  workflow.

## Why Redis Streams

Redis Streams (introduced in Redis 5.0) provides exactly the primitives
sagakit needs, without the operational overhead of a dedicated broker:

- **Consumer groups** with acknowledgement semantics — a step event is not
  removed from the stream until a worker explicitly acknowledges it. If a
  worker crashes, the message is re-delivered to another worker after a
  configurable timeout. This is the foundation of sagakit's failure
  recovery.
- **Durable persistence** — messages survive Redis restarts when Redis is
  configured with AOF or RDB persistence. This is sufficient for the
  durability guarantees sagakit offers.
- **Single-command local setup.** The entire infrastructure requirement for
  developing with sagakit is:
  ```bash
  docker run -d -p 6379:6379 redis
  ```
  No extra containers, no configuration files, no accounts. A developer can
  have sagakit running end-to-end in minutes.
- **Already in most developers' stacks.** Redis is the most widely deployed
  in-memory data store in the Python ecosystem. Many teams already run it
  for caching or session storage, meaning sagakit may require *zero new
  infrastructure* for existing users.
- **State store reuse.** sagakit uses Redis both as a transport (via Streams)
  and as a saga state store (via Redis hashes). This means a single Redis
  connection and a single infrastructure component serves both purposes,
  keeping the operational surface small.
- **Excellent async Python support.** `redis-py` provides a mature async
  client (`redis.asyncio`) that integrates cleanly with `asyncio`, which
  is sagakit's execution model.

## Consequences

### Positive

- Developer onboarding reduces to one `docker run` command.
- No cloud account or credentials required to run locally or in CI.
- Library remains vendor-neutral and cloud-agnostic.
- Minimal dependencies: one client library (`redis`) covers both transport
  and state storage.

### Negative — and these are real

- **Redis is not a purpose-built message broker.** It lacks some guarantees
  that Kafka or RabbitMQ provide by default: no built-in dead-letter queues
  (sagakit implements its own), no schema registry, no message ordering
  guarantees across partitions (Redis Streams are single-partition per key).
  Users who need those guarantees at scale should consider contributing a
  Kafka transport adapter.
- **Redis persistence is optional and off by default.** A vanilla `docker
  run redis` with no persistence flags will lose all in-flight saga state
  on restart. The library documentation must make this explicit and provide
  a recommended Redis configuration for production use.
- **Stream length management is the user's responsibility.** Redis Streams
  grow unboundedly unless trimmed with `MAXLEN`. sagakit will trim
  acknowledged entries automatically, but users must monitor stream health
  in production.
- **Single-node Redis is a single point of failure.** Redis Sentinel or
  Redis Cluster are the production answer, but they add operational
  complexity. The library cannot solve this for the user — it can only
  document it clearly.

### Neutral

- Users running sagakit in production on cloud infrastructure will likely
  want to contribute or use a managed-Redis adapter (ElastiCache, Azure
  Cache for Redis, Upstash). The `Transport` ABC makes this straightforward
  without requiring changes to the core library.

## Out of scope

- Redis Cluster support in v1. Single-node Redis (or Sentinel) is the
  assumed topology.
- Message schema validation or a schema registry equivalent. Step payloads
  are plain Python dicts serialized to JSON.
- Stream compaction or event sourcing patterns. sagakit uses streams as a
  task queue, not as an event log.

## References

- Redis Streams introduction: https://redis.io/docs/data-types/streams/
- Consumer groups documentation: https://redis.io/docs/data-types/streams-tutorial/
- `redis-py` async client: https://redis-py.readthedocs.io/en/stable/examples/asyncio_examples.html
