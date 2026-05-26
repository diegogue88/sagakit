from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

import structlog

T = TypeVar("T")


@dataclass(frozen=True)
class SagaContext(Generic[T]):
    """Immutable execution context passed to every step and compensation handler.

    Carries all information a step needs to do its work and remain idempotent.
    Construct exclusively via :meth:`create` — never instantiate directly.

    Example:
        ctx = SagaContext.create(
            saga_id="ord-123",
            saga_name="order_saga",
            step_name="reserve_inventory",
            attempt_number=1,
            saga_input=order_payload,
            step_results={},
        )
        print(ctx.idempotency_key)  # "ord-123:reserve_inventory:1"
    """

    saga_id: str
    saga_name: str
    step_name: str
    attempt_number: int
    idempotency_key: str
    saga_input: T
    step_results: dict[str, Any]
    metadata: dict[str, Any]
    logger: structlog.stdlib.BoundLogger

    @classmethod
    def create(
        cls,
        *,
        saga_id: str,
        saga_name: str,
        step_name: str,
        attempt_number: int,
        saga_input: T,
        step_results: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        logger: structlog.stdlib.BoundLogger | None = None,
    ) -> SagaContext[T]:
        """Build a context, computing the idempotency key and deep-copying mutable fields.

        Args:
            saga_id: Unique identifier for this saga instance.
            saga_name: Name of the saga definition being executed.
            step_name: Name of the step this context belongs to.
            attempt_number: 1-based retry count for this step.
            saga_input: The original input to the saga (not copied — caller owns it).
            step_results: Results accumulated by prior steps; deep-copied on creation.
            metadata: Arbitrary key/value bag for cross-cutting concerns; deep-copied.
            logger: Bound structlog logger; a default is created if omitted.
        """
        bound_logger: structlog.stdlib.BoundLogger = logger or structlog.get_logger().bind(
            saga_id=saga_id,
            saga_name=saga_name,
            step_name=step_name,
        )
        return cls(
            saga_id=saga_id,
            saga_name=saga_name,
            step_name=step_name,
            attempt_number=attempt_number,
            idempotency_key=f"{saga_id}:{step_name}:{attempt_number}",
            saga_input=saga_input,
            step_results=copy.deepcopy(step_results),
            metadata=copy.deepcopy(metadata or {}),
            logger=bound_logger,
        )
