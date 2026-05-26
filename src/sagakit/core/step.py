from __future__ import annotations

from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, overload

AsyncStepFn = Callable[..., Coroutine[Any, Any, Any]]


@dataclass(frozen=True)
class Step:
    """A single unit of work in a saga, paired with an optional compensation reference.

    Create instances with the :func:`step` decorator rather than constructing directly.

    Example:
        @step
        async def reserve_inventory(ctx: SagaContext[Order]) -> dict[str, Any]:
            ...

        @step(compensate="release_inventory")
        async def charge_payment(ctx: SagaContext[Order]) -> dict[str, Any]:
            ...
    """

    fn: AsyncStepFn
    name: str
    compensate_name: str | None


@overload
def step(__fn: AsyncStepFn) -> Step: ...


@overload
def step(*, compensate: str) -> Callable[[AsyncStepFn], Step]: ...


def step(
    __fn: AsyncStepFn | None = None,
    *,
    compensate: str | None = None,
) -> Step | Callable[[AsyncStepFn], Step]:
    """Decorator that converts an async function into a :class:`Step`.

    Can be used bare or with the ``compensate`` keyword argument.

    Args:
        compensate: Name of the step function that compensates this one on rollback.

    Example:
        @step
        async def create_order(ctx): ...

        @step(compensate="cancel_order")
        async def confirm_order(ctx): ...
    """

    def _make(fn: AsyncStepFn) -> Step:
        return Step(fn=fn, name=fn.__name__, compensate_name=compensate)

    if __fn is not None:
        return _make(__fn)

    return _make
