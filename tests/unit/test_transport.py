from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from sagakit.transport.redis_transport import RedisStreamsTransport


def _make_transport() -> tuple[RedisStreamsTransport, MagicMock]:
    client = MagicMock()
    client.xadd = AsyncMock()
    client.xack = AsyncMock()
    client.xrange = AsyncMock()
    client.xreadgroup = AsyncMock()
    client.xgroup_create = AsyncMock()
    transport = RedisStreamsTransport(client)
    return transport, client


@pytest.mark.asyncio
async def test_publish_returns_message_id() -> None:
    transport, client = _make_transport()
    client.xadd.return_value = "1700000000000-0"

    result = await transport.publish("orders", {"order_id": "abc"})

    assert result == "1700000000000-0"
    client.xadd.assert_awaited_once()
    args = client.xadd.call_args
    assert args[0][0] == "orders"
    assert args[0][1] == {"order_id": "abc"}


@pytest.mark.asyncio
async def test_acknowledge_calls_xack() -> None:
    transport, client = _make_transport()

    await transport.acknowledge("orders", "saga-workers", "1700000000000-0")

    client.xack.assert_awaited_once_with("orders", "saga-workers", "1700000000000-0")


@pytest.mark.asyncio
async def test_reject_requeue_acks_and_republishes() -> None:
    transport, client = _make_transport()
    client.xrange.return_value = [
        (
            b"1700000000000-0",
            {b"order_id": b"abc", b"attr:requeue_count": b"1"},
        )
    ]
    client.xadd.return_value = "1700000000001-0"

    await transport.reject("orders", "saga-workers", "1700000000000-0", requeue=True)

    # Must re-publish to the SAME stream with incremented requeue_count
    client.xadd.assert_awaited_once()
    xadd_args = client.xadd.call_args[0]
    assert xadd_args[0] == "orders"
    assert xadd_args[1]["attr:requeue_count"] == "2"
    assert xadd_args[1]["order_id"] == "abc"

    # Must ACK the original message AFTER re-publishing
    client.xack.assert_awaited_once_with("orders", "saga-workers", "1700000000000-0")


@pytest.mark.asyncio
async def test_reject_requeue_defaults_requeue_count_from_zero() -> None:
    transport, client = _make_transport()
    client.xrange.return_value = [(b"1700000000000-0", {b"order_id": b"abc"})]
    client.xadd.return_value = "1700000000001-0"

    await transport.reject("orders", "saga-workers", "1700000000000-0", requeue=True)

    xadd_args = client.xadd.call_args[0]
    assert xadd_args[1]["attr:requeue_count"] == "1"


@pytest.mark.asyncio
async def test_reject_no_requeue_writes_to_dlq_stream() -> None:
    transport, client = _make_transport()
    client.xrange.return_value = [(b"1700000000000-0", {b"order_id": b"abc"})]
    client.xadd.return_value = "1700000000001-0"

    await transport.reject("orders", "saga-workers", "1700000000000-0", requeue=False)

    # Must write to DLQ stream, not the original
    client.xadd.assert_awaited_once()
    xadd_args = client.xadd.call_args[0]
    assert xadd_args[0] == "orders:dlq"
    assert xadd_args[1]["attr:original_id"] == "1700000000000-0"

    # Must ACK original after writing to DLQ
    client.xack.assert_awaited_once_with("orders", "saga-workers", "1700000000000-0")


@pytest.mark.asyncio
async def test_initialize_ignores_existing_group_error() -> None:
    from redis.exceptions import ResponseError

    transport, client = _make_transport()
    client.xgroup_create.side_effect = ResponseError("BUSYGROUP Consumer Group name already exists")

    # Must not raise
    await transport.initialize("orders", "saga-workers")

    client.xgroup_create.assert_awaited_once_with("orders", "saga-workers", id="0", mkstream=True)


@pytest.mark.asyncio
async def test_initialize_propagates_unexpected_redis_error() -> None:
    from redis.exceptions import ResponseError

    transport, client = _make_transport()
    client.xgroup_create.side_effect = ResponseError("ERR Some other error")

    with pytest.raises(ResponseError):
        await transport.initialize("orders", "saga-workers")
