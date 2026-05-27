from __future__ import annotations

import asyncio

import pytest
import redis.asyncio as aioredis
from testcontainers.redis import RedisContainer

from sagakit.storage.redis_storage import RedisIdempotencyStore, RedisStateStore
from sagakit.transport.redis_transport import RedisStreamsTransport


@pytest.fixture(scope="module")
def redis_url() -> str:
    with RedisContainer() as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}"


@pytest.fixture
async def redis_client(redis_url: str) -> aioredis.Redis:  # type: ignore[type-arg]
    client = aioredis.from_url(redis_url, decode_responses=False)
    yield client
    await client.aclose()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_publish_and_consume_roundtrip(
    redis_client: aioredis.Redis,  # type: ignore[type-arg]
) -> None:
    transport = RedisStreamsTransport(redis_client)
    await transport.initialize("test:orders", "workers")

    msg_id = await transport.publish("test:orders", {"order_id": "xyz"})
    assert msg_id

    received: list = []

    async def _consume() -> None:
        async for msg in transport.consume("test:orders", "workers", "consumer-1"):
            received.append(msg)
            await transport.acknowledge("test:orders", "workers", msg.id)
            return

    await asyncio.wait_for(_consume(), timeout=5)

    assert len(received) == 1
    assert received[0].payload["order_id"] == "xyz"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_idempotency_prevents_double_processing(
    redis_client: aioredis.Redis,  # type: ignore[type-arg]
) -> None:
    store = RedisIdempotencyStore(redis_client)

    first = await store.set_processing("evt-001", ttl=60)
    second = await store.set_processing("evt-001", ttl=60)

    assert first is True
    assert second is False

    await store.set_completed("evt-001")
    status = await store.get_status("evt-001")
    assert status == "completed"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_state_store_save_load_delete(
    redis_client: aioredis.Redis,  # type: ignore[type-arg]
) -> None:
    store = RedisStateStore(redis_client)
    state = {"step": "charge_payment", "amount": 42}

    await store.save("saga-int-1", state)
    loaded = await store.load("saga-int-1")
    assert loaded == state

    await store.delete("saga-int-1")
    after_delete = await store.load("saga-int-1")
    assert after_delete is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reject_requeue_redelivers_message(
    redis_client: aioredis.Redis,  # type: ignore[type-arg]
) -> None:
    transport = RedisStreamsTransport(redis_client)
    await transport.initialize("test:requeue", "workers")

    await transport.publish("test:requeue", {"task": "do_thing"})

    first_received = None

    async def _consume_one() -> None:
        nonlocal first_received
        async for msg in transport.consume("test:requeue", "workers", "consumer-1"):
            first_received = msg
            return

    await asyncio.wait_for(_consume_one(), timeout=5)
    assert first_received is not None

    await transport.reject("test:requeue", "workers", first_received.id, requeue=True)

    redelivered = None

    async def _consume_redelivered() -> None:
        nonlocal redelivered
        async for msg in transport.consume("test:requeue", "workers", "consumer-2"):
            redelivered = msg
            await transport.acknowledge("test:requeue", "workers", msg.id)
            return

    await asyncio.wait_for(_consume_redelivered(), timeout=5)
    assert redelivered is not None
    assert redelivered.attributes.get("requeue_count") == "1"
    assert redelivered.payload["task"] == "do_thing"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reject_no_requeue_goes_to_dlq(
    redis_client: aioredis.Redis,  # type: ignore[type-arg]
) -> None:
    transport = RedisStreamsTransport(redis_client)
    await transport.initialize("test:dlq-source", "workers")

    await transport.publish("test:dlq-source", {"task": "failing_task"})

    consumed = None

    async def _consume_one() -> None:
        nonlocal consumed
        async for msg in transport.consume("test:dlq-source", "workers", "consumer-1"):
            consumed = msg
            return

    await asyncio.wait_for(_consume_one(), timeout=5)
    assert consumed is not None

    await transport.reject("test:dlq-source", "workers", consumed.id, requeue=False)

    # Verify message landed in DLQ
    dlq_entries = await redis_client.xrange("test:dlq-source:dlq", "-", "+")
    assert len(dlq_entries) == 1
    _id, fields = dlq_entries[0]
    raw_fields = {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in fields.items()
    }
    assert raw_fields.get("attr:original_id") == consumed.id
