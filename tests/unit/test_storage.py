from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from sagakit.storage.redis_storage import RedisIdempotencyStore, RedisStateStore


def _make_state_store() -> tuple[RedisStateStore, MagicMock]:
    client = MagicMock()
    client.hset = AsyncMock()
    client.hgetall = AsyncMock()
    client.delete = AsyncMock()
    return RedisStateStore(client), client


def _make_idempotency_store() -> tuple[RedisIdempotencyStore, MagicMock]:
    client = MagicMock()
    client.set = AsyncMock()
    client.get = AsyncMock()
    return RedisIdempotencyStore(client), client


@pytest.mark.asyncio
async def test_idempotency_set_processing_returns_true_when_key_is_new() -> None:
    store, client = _make_idempotency_store()
    client.set.return_value = True  # Redis SET NX returns the string "OK" / True on success

    result = await store.set_processing("msg-abc", ttl=3600)

    assert result is True
    client.set.assert_awaited_once_with(
        "sagakit:idempotency:msg-abc", "processing", nx=True, ex=3600
    )


@pytest.mark.asyncio
async def test_idempotency_set_processing_returns_false_when_key_exists() -> None:
    store, client = _make_idempotency_store()
    client.set.return_value = None  # Redis SET NX returns None when key already exists

    result = await store.set_processing("msg-abc", ttl=3600)

    assert result is False


@pytest.mark.asyncio
async def test_state_save_and_load_roundtrip() -> None:
    import json

    store, client = _make_state_store()
    state = {"step": "reserve_inventory", "retries": 2}
    # HGETALL returns bytes keys/values as Redis would
    client.hgetall.return_value = {
        b"step": json.dumps("reserve_inventory").encode(),
        b"retries": json.dumps(2).encode(),
    }

    await store.save("saga-1", state)
    loaded = await store.load("saga-1")

    client.hset.assert_awaited_once()
    hset_args = client.hset.call_args
    assert hset_args[0][0] == "sagakit:state:saga-1"
    assert json.loads(hset_args[1]["mapping"]["step"]) == "reserve_inventory"

    assert loaded == state


@pytest.mark.asyncio
async def test_state_load_returns_none_for_missing_key() -> None:
    store, client = _make_state_store()
    client.hgetall.return_value = {}  # Redis returns empty dict for a missing hash

    result = await store.load("saga-nonexistent")

    assert result is None
