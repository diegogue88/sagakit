# CLAUDE.md

This file is read by Claude Code at the start of every session. It is the
source of truth for project conventions. If a user request conflicts with
this file, raise the conflict before acting.

## Project overview

**sagakit** is a small, focused Python library for orchestrating distributed
transactions using the **Saga pattern**. Target users: Python backend
engineers building event-driven systems who need durable, compensable
multi-step workflows without reaching for Temporal or a full workflow engine.

The project is also a public portfolio piece. Code quality, documentation
quality, and clarity of design decisions matter as much as functionality.
Optimize for "a senior engineer reads the repo and is impressed" — not for
feature count.

## Non-negotiable principles

1. **Saga pattern, orchestrated execution only.** Two-phase commit and
   choreography are explicitly out of scope (see `docs/adr/001-why-sagas-over-2pc.md`).
2. **Async-first.** All public APIs are `async`. No threading, no sync
   wrappers in v1.
3. **One transport in v1: Redis Streams.** The `Transport` interface must be
   an ABC so other transports can be added later without breaking changes,
   but do not implement RabbitMQ, Kafka, or Service Bus yet.
4. **Minimal dependencies.** Core depends only on the Python stdlib +
   `redis` (async client) + `structlog`. No `pydantic`, no `attrs`, no web
   frameworks. Dataclasses are enough.
5. **Type-checked, strict.** `mypy --strict` must pass on `src/`. No
   `# type: ignore` without a comment justifying it.
6. **Tests are part of the deliverable, not an afterthought.** Every public
   API has unit tests. Transport and persistence layers have integration
   tests using `testcontainers`.
7. **No silent failures.** Errors surface as exceptions or logged events
   with structured context. Never swallow exceptions.

## Tech stack

- Python 3.11+
- Package manager: `uv`
- Linter/formatter: `ruff` (replaces black, isort, flake8)
- Type checker: `mypy` in strict mode
- Test framework: `pytest` + `pytest-asyncio` + `testcontainers`
- CI: GitHub Actions
- License: MIT

## Repo structure (authoritative)

```
sagakit/
├── README.md
├── ARCHITECTURE.md
├── CLAUDE.md                  ← this file
├── LICENSE
├── pyproject.toml
├── .github/workflows/ci.yml
├── docs/
│   └── adr/                   ← Architecture Decision Records (MADR format)
├── src/sagakit/
│   ├── __init__.py
│   ├── core/                  ← Saga, Step, SagaContext (pure, no I/O)
│   ├── transport/             ← Transport ABC + implementations
│   ├── storage/               ← saga state persistence
│   ├── retry.py
│   ├── idempotency.py
│   └── observability.py
├── examples/
│   └── order_processing/      ← runnable end-to-end example
└── tests/
    ├── unit/
    └── integration/
```

Do not introduce new top-level directories without updating this file first.

## Coding conventions

- **Public API:** documented with docstrings in Google style. Every public
  function/class has at least one usage example in its docstring.
- **Private functions:** prefixed with `_`. No leaking implementation
  details through public modules.
- **Imports:** absolute imports inside the package (`from sagakit.core
  import Saga`), never relative.
- **Errors:** custom exception hierarchy rooted at `SagaError`. Never raise
  bare `Exception`. Never catch bare `Exception` except at the outermost
  worker loop, and even there, log structured context and re-raise unless
  the loop must continue.
- **Logging:** `structlog` only. No `print`, no stdlib `logging` calls in
  library code. Tests may use `print` for debugging during development but
  not in committed code.
- **No comments that restate the code.** A comment explains *why*, not
  *what*. If the code needs a "what" comment, the code is unclear — rewrite
  it instead.
- **Type hints:** required on every function signature, public and private.
- **Line length:** 100 characters.

## What NOT to do

- Do **not** add YAML or TOML-based saga configuration. Sagas are defined
  in Python code. This is a deliberate API choice.
- Do **not** add a CLI in v1. The library is consumed as a Python import.
- Do **not** add web framework integrations (FastAPI, Flask) to the core
  package. If needed later, they live in a separate `sagakit-fastapi`
  package.
- Do **not** generate placeholder code with `TODO` or `pass`. Either
  implement the thing or do not create the file yet.
- Do **not** write tests that only assert the code does what the code does
  (tautological tests). Tests must encode a behavioural contract.
- Do **not** auto-generate docstrings that paraphrase the function name.
  If there is nothing meaningful to say, leave it out and let the type
  hints speak.

## Architectural Decision Records (ADRs)

ADRs live in `docs/adr/` and follow MADR format. Existing decisions:

- `001-why-sagas-over-2pc.md` — Accepted. Sagas chosen over 2PC.

Pending ADRs (to be written before the relevant sprint):

- `002-redis-streams-as-default-transport.md`
- `003-idempotency-strategy.md`
- `004-compensation-semantics.md`

**Before implementing a major component, check whether its ADR exists. If
not, stop and tell the user — the ADR is written first, by the user, not
generated.**

## Working agreement with Claude Code

- Work in **small, reviewable commits**. One logical change per commit.
  Conventional Commits style (`feat:`, `fix:`, `docs:`, `test:`, `chore:`,
  `refactor:`).
- After implementing anything non-trivial, run `uv run ruff check`,
  `uv run ruff format`, `uv run mypy --strict src/`, and `uv run pytest`
  before declaring it done. If any of those fail, fix them; do not hand
  back broken state. Never invoke `pytest`, `mypy`, or `ruff` directly —
  always prefix with `uv run` to ensure the project's venv is used, not
  the global Python installation.
- Never add `Co-Authored-By: Claude` or any AI attribution to commit
  messages. All commits are authored by the repo owner alone.
- When the user asks for a feature, **first restate the plan in 3-5 bullets
  and wait for confirmation** before writing code. Do not over-engineer.
- If a request seems to contradict this file, raise the conflict. Do not
  silently override conventions.
- Do not write code on behalf of the user for parts marked as "user
  writes": ADRs, the main README narrative, the "Why sagakit vs X" section,
  and the "When NOT to use sagakit" section. You may critique drafts of
  these, but do not generate them from scratch.

## Out of scope for v1

- Multiple transports (Redis Streams only)
- Choreographed sagas
- Nested or cross-saga transactions
- Distributed locking primitives
- Web UI / dashboard
- Non-Python clients
