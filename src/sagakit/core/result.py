from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SagaStatus(str, Enum):
    """Terminal state of a saga execution.

    Inherits from ``str`` so values can be stored as plain strings in logs
    and persistence layers without a separate serialisation step.

    Example:
        result.status == SagaStatus.COMPLETED  # True
        result.status == "COMPLETED"           # also True
    """

    COMPLETED = "COMPLETED"
    COMPENSATED = "COMPENSATED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class SagaResult:
    """Immutable record of a saga execution's final outcome.

    Produced by the saga executor after all steps have run (or all compensations
    have been attempted). Inspect :attr:`status` to branch on success vs rollback.

    Example:
        if result.status == SagaStatus.COMPLETED:
            publish_order_confirmed(result.step_results["confirm_order"])
        elif result.status == SagaStatus.COMPENSATED:
            notify_customer_of_failure(result.failed_step)
    """

    saga_id: str
    status: SagaStatus
    step_results: dict[str, Any]
    failed_step: str | None
    compensated_steps: list[str] = field(default_factory=list)
    error: Exception | None = None
