from __future__ import annotations

import dataclasses

import pytest

from sagakit.core.context import SagaContext


def _make(**kwargs: object) -> SagaContext[dict[str, str]]:
    defaults: dict[str, object] = dict(
        saga_id="saga-1",
        saga_name="order_saga",
        step_name="reserve_inventory",
        attempt_number=1,
        saga_input={"order_id": "ord-99"},
        step_results={},
    )
    defaults.update(kwargs)
    return SagaContext.create(**defaults)  # type: ignore[arg-type]


class TestImmutability:
    def test_frozen_raises_on_field_assignment(self) -> None:
        ctx = _make()
        with pytest.raises(dataclasses.FrozenInstanceError):
            ctx.saga_id = "other"  # type: ignore[misc]

    def test_frozen_raises_on_attempt_number_assignment(self) -> None:
        ctx = _make()
        with pytest.raises(dataclasses.FrozenInstanceError):
            ctx.attempt_number = 99  # type: ignore[misc]


class TestDeepCopy:
    def test_mutating_source_dict_does_not_affect_context(self) -> None:
        source: dict[str, object] = {"charge": {"amount": 100}}
        ctx = _make(step_results=source)
        source["charge"] = {"amount": 999}
        assert ctx.step_results["charge"] == {"amount": 100}

    def test_mutating_nested_value_does_not_affect_context(self) -> None:
        nested: dict[str, object] = {"amount": 100}
        source: dict[str, object] = {"charge": nested}
        ctx = _make(step_results=source)
        nested["amount"] = 999
        assert ctx.step_results["charge"] == {"amount": 100}  # type: ignore[index]


class TestIdempotencyKey:
    def test_key_format(self) -> None:
        ctx = _make(saga_id="abc", step_name="do_thing", attempt_number=3)
        assert ctx.idempotency_key == "abc:do_thing"

    def test_key_changes_with_attempt_number(self) -> None:
        ctx1 = _make(attempt_number=1)
        ctx2 = _make(attempt_number=2)
        assert ctx1.idempotency_key == ctx2.idempotency_key

    def test_key_changes_with_step_name(self) -> None:
        ctx1 = _make(step_name="step_a")
        ctx2 = _make(step_name="step_b")
        assert ctx1.idempotency_key != ctx2.idempotency_key
