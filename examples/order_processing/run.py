"""Order processing saga — runnable end-to-end example.

Usage:
    # Happy path
    python run.py

    # Simulate failure at a specific step
    FAIL_AT_STEP=ship_order python run.py

Environment variables:
    REDIS_URL     Redis connection string (default: redis://localhost:6379)
    FAIL_AT_STEP  Step name to inject a RuntimeError into (default: none)
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import redis.asyncio as aioredis
import structlog
from steps import (
    charge_payment,
    refund_payment,
    release_inventory,
    reserve_inventory,
    ship_order,
)

from sagakit import (
    RedisIdempotencyStore,
    RedisStateStore,
    RedisStreamsTransport,
    Saga,
    SagaConfig,
    SagaExecutor,
    SagaStatus,
)

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.PrintLoggerFactory(),
)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")

order_saga: Saga[dict] = Saga(
    name="order_processing",
    steps=[charge_payment, reserve_inventory, ship_order],
    compensations=[refund_payment, release_inventory],
)

PAYLOAD = {
    "order_id": "ord-001",
    "amount": 99.99,
    "items": ["item-a", "item-b"],
}


async def main() -> None:
    client: aioredis.Redis = aioredis.from_url(REDIS_URL)
    try:
        config = SagaConfig(
            transport=RedisStreamsTransport(client),
            state_store=RedisStateStore(client),
            idempotency_store=RedisIdempotencyStore(client),
            max_attempts=3,
            retry_base_delay=0.1,
            retry_max_delay=1.0,
        )

        executor: SagaExecutor[dict] = SagaExecutor(config)
        result = await executor.execute(order_saga, PAYLOAD)

        print()
        print(f"Status   : {result.status}")
        print(f"Saga ID  : {result.saga_id}")

        if result.step_results:
            print("Results  :")
            for step_name, step_result in result.step_results.items():
                print(f"  {step_name}: {step_result}")

        if result.status in (SagaStatus.COMPENSATED, SagaStatus.FAILED):
            print(f"Failed at: {result.failed_step}")
            print(f"Error    : {result.error}")
            if result.compensated_steps:
                print(f"Rolled back: {', '.join(result.compensated_steps)}")

        sys.exit(0 if result.status == SagaStatus.COMPLETED else 1)
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
