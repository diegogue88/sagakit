from __future__ import annotations

import warnings
from typing import Any

import pytest

from sagakit.core.saga import Saga


async def _noop(ctx: Any) -> dict[str, Any]:
    return {}


def _make_step(name: str, compensate: str | None = None) -> Any:
    fn = type("_Fn", (), {"__name__": name, "__call__": _noop})()
    fn.__name__ = name

    async def impl(ctx: Any) -> dict[str, Any]:
        return {}

    impl.__name__ = name
    from sagakit.core.step import Step

    return Step(fn=impl, name=name, compensate_name=compensate)


class TestValidSaga:
    def test_saga_with_compensations_constructs_without_error(self) -> None:
        reserve = _make_step("reserve_inventory", compensate="release_inventory")
        release = _make_step("release_inventory")
        saga = Saga(name="order_saga", steps=[reserve, release])
        assert saga.name == "order_saga"
        assert len(saga.steps) == 2

    def test_single_step_saga_has_no_warning(self) -> None:
        confirm = _make_step("confirm_order")
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            Saga(name="order_saga", steps=[confirm])

    def test_final_step_without_compensation_has_no_warning(self) -> None:
        # Only the last step; non-final steps all have compensation so no warning fires.
        reserve = _make_step("reserve_inventory", compensate="release_inventory")
        release = _make_step("release_inventory")  # final — no compensation needed
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            Saga(name="order_saga", steps=[reserve, release])


class TestValidationErrors:
    def test_self_compensation_raises_value_error(self) -> None:
        loop_step = _make_step("charge_payment", compensate="charge_payment")
        with pytest.raises(ValueError, match="infinite loop"):
            Saga(name="order_saga", steps=[loop_step])

    def test_invalid_compensate_name_raises_value_error(self) -> None:
        bad = _make_step("charge_payment", compensate="nonexistent_step")
        with pytest.raises(ValueError, match="nonexistent_step") as exc_info:
            Saga(name="order_saga", steps=[bad])
        assert "does not exist" in str(exc_info.value)


class TestCompensationWarnings:
    def test_non_final_step_without_compensation_emits_warning(self) -> None:
        unguarded = _make_step("reserve_inventory")  # no compensate_name
        final = _make_step("confirm_order")
        with pytest.warns(UserWarning, match="reserve_inventory"):
            Saga(name="order_saga", steps=[unguarded, final])

    def test_warning_message_mentions_saga_name(self) -> None:
        unguarded = _make_step("do_thing")
        final = _make_step("finish")
        with pytest.warns(UserWarning, match="my_saga"):
            Saga(name="my_saga", steps=[unguarded, final])
