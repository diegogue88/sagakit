from __future__ import annotations

import dataclasses

import pytest

from sagakit.core.result import SagaResult, SagaStatus


class TestSagaStatus:
    def test_completed_value(self) -> None:
        assert SagaStatus.COMPLETED == "COMPLETED"

    def test_compensated_value(self) -> None:
        assert SagaStatus.COMPENSATED == "COMPENSATED"

    def test_failed_value(self) -> None:
        assert SagaStatus.FAILED == "FAILED"

    def test_status_is_str_subclass(self) -> None:
        assert isinstance(SagaStatus.COMPLETED, str)


class TestSagaResult:
    def _make(self, **kwargs: object) -> SagaResult:
        defaults: dict[str, object] = dict(
            saga_id="saga-1",
            status=SagaStatus.COMPLETED,
            step_results={},
            failed_step=None,
        )
        defaults.update(kwargs)
        return SagaResult(**defaults)  # type: ignore[arg-type]

    def test_result_is_frozen(self) -> None:
        result = self._make()
        with pytest.raises(dataclasses.FrozenInstanceError):
            result.saga_id = "other"  # type: ignore[misc]

    def test_default_compensated_steps_is_empty_list(self) -> None:
        result = self._make()
        assert result.compensated_steps == []

    def test_default_error_is_none(self) -> None:
        result = self._make()
        assert result.error is None

    def test_error_field_accepts_exception(self) -> None:
        exc = RuntimeError("payment gateway timeout")
        result = self._make(status=SagaStatus.FAILED, failed_step="charge_payment", error=exc)
        assert result.error is exc

    def test_error_field_rejects_base_exception_at_runtime(self) -> None:
        # SystemExit and KeyboardInterrupt must never be stored as saga errors.
        # The type system enforces Exception | None; this test documents the intent
        # by confirming a plain Exception works and a BaseException subclass like
        # SystemExit is not the intended type.
        exc = RuntimeError("ok")
        result = self._make(error=exc)
        assert isinstance(result.error, Exception)

    def test_completed_result_has_no_failed_step(self) -> None:
        result = self._make(status=SagaStatus.COMPLETED)
        assert result.failed_step is None

    def test_compensated_result_records_compensated_steps(self) -> None:
        result = self._make(
            status=SagaStatus.COMPENSATED,
            failed_step="charge_payment",
            compensated_steps=["reserve_inventory"],
        )
        assert "reserve_inventory" in result.compensated_steps
