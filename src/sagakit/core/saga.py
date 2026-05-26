from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Generic, TypeVar

from sagakit.core.step import Step

T = TypeVar("T")


@dataclass
class Saga(Generic[T]):
    """An ordered sequence of steps that form a distributed transaction.

    Steps are executed in list order. Each step may declare a compensation
    handler by name; if execution fails, sagakit runs compensations in reverse
    order for all steps that already completed.

    Validation runs automatically at construction time and will raise
    :class:`ValueError` for self-referential or dangling compensation references.
    Steps that can be reached mid-saga without a compensation handler emit a
    :class:`UserWarning` — this is legal but risky.

    Example:
        saga = Saga(
            name="order_saga",
            steps=[reserve_inventory_step, charge_payment_step, confirm_order_step],
        )
    """

    name: str
    steps: list[Step] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        step_names = {s.name for s in self.steps}

        for s in self.steps:
            if s.compensate_name is None:
                continue
            if s.compensate_name == s.name:
                raise ValueError(
                    f"Step '{s.name}' references itself as its compensation handler, "
                    "which would cause an infinite loop."
                )
            if s.compensate_name not in step_names:
                raise ValueError(
                    f"Step '{s.name}' declares compensate_name='{s.compensate_name}', "
                    f"but '{s.compensate_name}' does not exist in this saga."
                )

        # Non-final steps without a compensation handler are legal but dangerous:
        # if a later step fails there is nothing to undo this step's side-effects.
        for s in self.steps[:-1]:
            if s.compensate_name is None:
                warnings.warn(
                    f"Step '{s.name}' in saga '{self.name}' has no compensation handler. "
                    "If a later step fails, this step's side-effects cannot be rolled back.",
                    UserWarning,
                    stacklevel=3,
                )
