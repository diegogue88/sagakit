from __future__ import annotations

import asyncio
import random
import uuid
from typing import Any, Generic, TypeVar

import structlog

from sagakit.core import Saga, SagaContext, SagaResult, SagaStatus
from sagakit.core.step import Step
from sagakit.executor.config import SagaConfig

T = TypeVar("T")


class SagaExecutor(Generic[T]):
    """Drives a :class:`~sagakit.core.Saga` to completion against live infrastructure.

    Handles the full execution lifecycle: forward step execution with
    idempotency checks, exponential-backoff retries, and reverse compensation
    when a step exhausts its retry budget.

    Example:
        executor = SagaExecutor(config)
        result = await executor.execute(order_saga, order_payload)
        if result.status == SagaStatus.COMPLETED:
            ...
    """

    def __init__(self, config: SagaConfig) -> None:
        self._config = config

    async def execute(
        self,
        saga: Saga[T],
        payload: T,
        *,
        saga_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SagaResult:
        """Execute all steps of *saga* in order, compensating on failure.

        Args:
            saga: The saga definition to run.
            payload: Input passed to every step via ``ctx.saga_input``.
            saga_id: Stable identifier for this execution; auto-generated if omitted.
            metadata: Arbitrary key/value bag forwarded to every :class:`~sagakit.core.SagaContext`.

        Returns:
            A :class:`~sagakit.core.SagaResult` describing the terminal state.
        """
        resolved_id = saga_id or uuid.uuid4().hex
        resolved_meta = metadata or {}
        step_results: dict[str, Any] = {}

        logger: structlog.stdlib.BoundLogger = structlog.get_logger().bind(
            saga_id=resolved_id,
            saga_name=saga.name,
        )

        completed_steps: list[Step] = []

        for step in saga.steps:
            success, result, exc = await self._run_step(
                step=step,
                saga_id=resolved_id,
                saga_name=saga.name,
                payload=payload,
                step_results=step_results,
                metadata=resolved_meta,
                logger=logger,
            )

            if success:
                step_results[step.name] = result
                completed_steps.append(step)
            else:
                assert exc is not None
                compensated, comp_names = await self._compensate(
                    completed_steps=completed_steps,
                    saga_id=resolved_id,
                    saga_name=saga.name,
                    saga_steps=saga.steps,
                    saga_compensations=saga.compensations,
                    payload=payload,
                    step_results=step_results,
                    metadata=resolved_meta,
                    logger=logger,
                )
                status = SagaStatus.COMPENSATED if compensated else SagaStatus.FAILED
                return SagaResult(
                    saga_id=resolved_id,
                    status=status,
                    step_results=step_results,
                    failed_step=step.name,
                    compensated_steps=comp_names,
                    error=exc,
                )

        return SagaResult(
            saga_id=resolved_id,
            status=SagaStatus.COMPLETED,
            step_results=step_results,
            failed_step=None,
            compensated_steps=[],
            error=None,
        )

    async def _run_step(
        self,
        *,
        step: Step,
        saga_id: str,
        saga_name: str,
        payload: T,
        step_results: dict[str, Any],
        metadata: dict[str, Any],
        logger: structlog.stdlib.BoundLogger,
        step_name_override: str | None = None,
    ) -> tuple[bool, Any, Exception | None]:
        """Attempt a single step (or compensation) with retry logic.

        Returns ``(success, result, exception)``.  On success, ``exception`` is
        ``None``; on exhausted retries, ``result`` is ``None``.
        """
        cfg = self._config
        effective_name = step_name_override or step.name
        attempt_number = 1

        while True:
            ctx = SagaContext.create(
                saga_id=saga_id,
                saga_name=saga_name,
                step_name=effective_name,
                attempt_number=attempt_number,
                saga_input=payload,
                step_results=step_results,
                metadata=metadata,
            )

            claimed = await cfg.idempotency_store.set_processing(
                key=ctx.idempotency_key,
                ttl=cfg.idempotency_ttl,
            )
            if not claimed:
                # Another worker already processed this step; load its result.
                saved = await cfg.state_store.load(saga_id)
                prior_result = saved.get(effective_name) if saved else None
                logger.info(
                    "step.skipped_idempotent",
                    step=effective_name,
                    attempt=attempt_number,
                )
                return True, prior_result, None

            try:
                result = await step.fn(ctx)
            except Exception as exc:
                await cfg.idempotency_store.set_failed(ctx.idempotency_key)
                if attempt_number < cfg.max_attempts:
                    delay = min(
                        cfg.retry_base_delay * (2 ** (attempt_number - 1)),
                        cfg.retry_max_delay,
                    ) * random.uniform(0.5, 1.5)
                    ctx.logger.warning(
                        "step.retrying",
                        step=effective_name,
                        attempt=attempt_number,
                        delay=delay,
                        error=str(exc),
                    )
                    await asyncio.sleep(delay)
                    attempt_number += 1
                    continue

                ctx.logger.error(
                    "step.exhausted_retries",
                    step=effective_name,
                    attempts=attempt_number,
                    error=str(exc),
                )
                return False, None, exc
            else:
                await cfg.idempotency_store.set_completed(ctx.idempotency_key)
                await cfg.state_store.save(saga_id, {effective_name: result})
                return True, result, None

    async def _compensate(
        self,
        *,
        completed_steps: list[Step],
        saga_id: str,
        saga_name: str,
        saga_steps: list[Step],
        saga_compensations: list[Step],
        payload: T,
        step_results: dict[str, Any],
        metadata: dict[str, Any],
        logger: structlog.stdlib.BoundLogger,
    ) -> tuple[bool, list[str]]:
        """Run compensations in reverse order for all completed steps.

        Returns ``(all_succeeded, list_of_compensated_step_names)``.
        ``all_succeeded`` is ``False`` if any compensation itself exhausted retries.
        """
        step_by_name = {s.name: s for s in (*saga_steps, *saga_compensations)}
        compensated_names: list[str] = []
        all_succeeded = True

        for original_step in reversed(completed_steps):
            if original_step.compensate_name is None:
                logger.warning(
                    "compensation.no_handler",
                    step=original_step.name,
                )
                continue

            comp_step = step_by_name.get(original_step.compensate_name)
            if comp_step is None:
                logger.error(
                    "compensation.handler_not_found",
                    step=original_step.name,
                    compensate_name=original_step.compensate_name,
                )
                all_succeeded = False
                continue

            comp_name = f"compensate_{original_step.name}"
            success, _, exc = await self._run_step(
                step=comp_step,
                saga_id=saga_id,
                saga_name=saga_name,
                payload=payload,
                step_results=step_results,
                metadata=metadata,
                logger=logger,
                step_name_override=comp_name,
            )

            if success:
                compensated_names.append(original_step.name)
            else:
                logger.error(
                    "compensation.exhausted_retries",
                    step=original_step.name,
                    compensation=comp_name,
                    error=str(exc),
                )
                all_succeeded = False

        return all_succeeded, compensated_names
