from __future__ import annotations

from dataclasses import dataclass, field

from sagakit.storage import IdempotencyStore, StateStore
from sagakit.transport import Transport


@dataclass
class SagaConfig:
    """Configuration bundle passed to :class:`~sagakit.executor.SagaExecutor`.

    Groups the three required infrastructure dependencies with the retry and
    idempotency tuning knobs so callers configure everything in one place.

    Example:
        config = SagaConfig(
            transport=redis_transport,
            state_store=redis_state_store,
            idempotency_store=redis_idempotency_store,
        )
    """

    transport: Transport
    state_store: StateStore
    idempotency_store: IdempotencyStore
    max_attempts: int = field(default=3)
    retry_base_delay: float = field(default=1.0)
    retry_max_delay: float = field(default=30.0)
    idempotency_ttl: int = field(default=86400)
