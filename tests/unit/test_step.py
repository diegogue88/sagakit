from __future__ import annotations

import asyncio
from typing import Any

import pytest

from sagakit.core.step import Step, step


class TestStepDecorator:
    def test_bare_decorator_produces_step_instance(self) -> None:
        @step
        async def do_work(ctx: Any) -> dict[str, Any]:
            return {}

        assert isinstance(do_work, Step)

    def test_bare_decorator_preserves_function_name(self) -> None:
        @step
        async def reserve_inventory(ctx: Any) -> dict[str, Any]:
            return {}

        assert reserve_inventory.name == "reserve_inventory"

    def test_bare_decorator_sets_no_compensate_name(self) -> None:
        @step
        async def do_work(ctx: Any) -> dict[str, Any]:
            return {}

        assert do_work.compensate_name is None

    def test_compensate_kwarg_sets_compensate_name(self) -> None:
        @step(compensate="release_inventory")
        async def reserve_inventory(ctx: Any) -> dict[str, Any]:
            return {}

        assert reserve_inventory.compensate_name == "release_inventory"

    def test_compensate_kwarg_preserves_function_name(self) -> None:
        @step(compensate="release_inventory")
        async def reserve_inventory(ctx: Any) -> dict[str, Any]:
            return {}

        assert reserve_inventory.name == "reserve_inventory"

    def test_fn_field_is_callable_as_coroutine(self) -> None:
        @step
        async def do_work(ctx: Any) -> str:
            return "done"

        result = asyncio.run(do_work.fn(None))
        assert result == "done"

    def test_step_is_frozen(self) -> None:
        import dataclasses

        @step
        async def do_work(ctx: Any) -> dict[str, Any]:
            return {}

        with pytest.raises(dataclasses.FrozenInstanceError):
            do_work.name = "other"  # type: ignore[misc]
