from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sagakit.core import Saga, SagaStatus, step
from sagakit.core.context import SagaContext
from sagakit.executor import SagaConfig, SagaExecutor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    *,
    max_attempts: int = 3,
    retry_base_delay: float = 0.0,
    retry_max_delay: float = 0.0,
) -> SagaConfig:
    """Return a SagaConfig wired with AsyncMock infrastructure."""
    transport = MagicMock()
    state_store = AsyncMock()
    idempotency_store = AsyncMock()

    # Default: set_processing returns True (key is new — proceed)
    idempotency_store.set_processing.return_value = True
    # Default: load returns an empty dict
    state_store.load.return_value = {}

    return SagaConfig(
        transport=transport,
        state_store=state_store,
        idempotency_store=idempotency_store,
        max_attempts=max_attempts,
        retry_base_delay=retry_base_delay,
        retry_max_delay=retry_max_delay,
        idempotency_ttl=60,
    )


@step
async def step_a(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
    return {"a": 1}


@step(compensate="compensate_a")
async def step_b(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
    return {"b": 2}


@step
async def compensate_a(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
    return {"rollback_a": True}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_successful_saga_returns_completed_status() -> None:
    config = _make_config()
    saga: Saga[dict[str, Any]] = Saga(name="test_saga", steps=[step_a])
    executor: SagaExecutor[dict[str, Any]] = SagaExecutor(config)

    result = await executor.execute(saga, {})

    assert result.status == SagaStatus.COMPLETED
    assert result.failed_step is None
    assert result.error is None
    assert result.step_results == {"step_a": {"a": 1}}


@pytest.mark.asyncio
async def test_saga_result_contains_all_step_results() -> None:
    config = _make_config()

    @step
    async def step_x(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        return {"x": 10}

    @step
    async def step_y(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        return {"y": 20}

    saga: Saga[dict[str, Any]] = Saga(name="multi_step", steps=[step_x, step_y])
    executor: SagaExecutor[dict[str, Any]] = SagaExecutor(config)

    result = await executor.execute(saga, {})

    assert result.status == SagaStatus.COMPLETED
    assert result.step_results["step_x"] == {"x": 10}
    assert result.step_results["step_y"] == {"y": 20}


@pytest.mark.asyncio
async def test_step_failure_triggers_compensation() -> None:
    config = _make_config(max_attempts=1)
    comp_mock = AsyncMock(return_value={"rolled_back": True})

    @step(compensate="undo_good")
    async def good_step(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        return {"ok": True}

    @step
    async def bad_step(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        raise ValueError("boom")

    @step
    async def undo_good(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        return await comp_mock(ctx)

    saga: Saga[dict[str, Any]] = Saga(name="comp_saga", steps=[good_step, bad_step, undo_good])
    executor: SagaExecutor[dict[str, Any]] = SagaExecutor(config)

    result = await executor.execute(saga, {})

    assert result.status == SagaStatus.COMPENSATED
    assert result.failed_step == "bad_step"
    assert "good_step" in result.compensated_steps
    assert isinstance(result.error, ValueError)
    comp_mock.assert_awaited_once()


@pytest.mark.asyncio
async def test_step_retries_before_compensating() -> None:
    call_count = 0

    @step(compensate="undo_retry")
    async def flaky_step(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        raise RuntimeError("transient")

    @step
    async def undo_retry(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        return {}

    config = _make_config(max_attempts=3, retry_base_delay=0.0, retry_max_delay=0.0)
    saga: Saga[dict[str, Any]] = Saga(name="retry_saga", steps=[flaky_step, undo_retry])
    executor: SagaExecutor[dict[str, Any]] = SagaExecutor(config)

    with patch("sagakit.executor.executor.asyncio.sleep", new_callable=AsyncMock):
        result = await executor.execute(saga, {})

    # 3 attempts before giving up
    assert call_count == 3
    assert result.status == SagaStatus.COMPENSATED


@pytest.mark.asyncio
async def test_idempotency_skip_already_processed_step() -> None:
    config = _make_config()
    # Simulate key already claimed by another worker
    config.idempotency_store.set_processing.return_value = False
    config.state_store.load.return_value = {"step_a": {"a": 99}}

    saga: Saga[dict[str, Any]] = Saga(name="idem_saga", steps=[step_a])
    executor: SagaExecutor[dict[str, Any]] = SagaExecutor(config)

    result = await executor.execute(saga, {})

    assert result.status == SagaStatus.COMPLETED
    # Result loaded from store, not re-executed
    assert result.step_results["step_a"] == {"a": 99}


@pytest.mark.asyncio
async def test_compensation_failure_returns_failed_status() -> None:
    @step(compensate="bad_compensation")
    async def forward_step(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        return {"ok": True}

    @step
    async def failing_forward(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        raise RuntimeError("forward failure")

    @step
    async def bad_compensation(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        raise RuntimeError("compensation also broken")

    config = _make_config(max_attempts=1)
    saga: Saga[dict[str, Any]] = Saga(
        name="broken_comp",
        steps=[forward_step, failing_forward, bad_compensation],
    )
    executor: SagaExecutor[dict[str, Any]] = SagaExecutor(config)

    result = await executor.execute(saga, {})

    assert result.status == SagaStatus.FAILED


@pytest.mark.asyncio
async def test_exponential_backoff_called_with_sleep() -> None:
    """asyncio.sleep is called with a value in the expected jitter range."""
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    @step(compensate="noop_comp")
    async def always_fails(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        raise RuntimeError("fail")

    @step
    async def noop_comp(ctx: SagaContext[dict[str, Any]]) -> dict[str, Any]:
        return {}

    config = _make_config(max_attempts=3, retry_base_delay=1.0, retry_max_delay=30.0)
    saga: Saga[dict[str, Any]] = Saga(name="backoff_saga", steps=[always_fails, noop_comp])
    executor: SagaExecutor[dict[str, Any]] = SagaExecutor(config)

    with patch("sagakit.executor.executor.asyncio.sleep", side_effect=fake_sleep):
        await executor.execute(saga, {})

    # Two sleeps: after attempt 1 and attempt 2 (attempt 3 exhausts)
    assert len(sleep_calls) == 2
    # Attempt 1 base: 1.0 * 2^0 = 1.0, jitter [0.5, 1.5]
    assert 0.5 <= sleep_calls[0] <= 1.5
    # Attempt 2 base: 1.0 * 2^1 = 2.0, jitter [1.0, 3.0]
    assert 1.0 <= sleep_calls[1] <= 3.0
