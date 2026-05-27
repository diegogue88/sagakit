from __future__ import annotations

import json
from typing import Any

import redis.asyncio as aioredis

from sagakit.storage.base import IdempotencyStore, StateStore

_STATE_KEY_PREFIX = "sagakit:state:"
_IDEMPOTENCY_KEY_PREFIX = "sagakit:idempotency:"
_DEFAULT_TTL = 86_400  # 24 hours

_STATUS_PROCESSING = "processing"
_STATUS_COMPLETED = "completed"
_STATUS_FAILED = "failed"


class RedisStateStore(StateStore):
    """Saga state persistence backed by Redis hashes.

    Each saga is stored as a hash at ``sagakit:state:{saga_id}``.  Values
    are JSON-serialised so the store is agnostic to payload shape.

    Example:
        import redis.asyncio as aioredis
        from sagakit.storage import RedisStateStore

        client = aioredis.from_url("redis://localhost:6379")
        store = RedisStateStore(client)
        await store.save("saga-1", {"step": "reserve_inventory", "status": "pending"})
        state = await store.load("saga-1")
    """

    def __init__(self, client: aioredis.Redis) -> None:  # type: ignore[type-arg]
        self._client = client

    def _key(self, saga_id: str) -> str:
        return f"{_STATE_KEY_PREFIX}{saga_id}"

    async def save(self, saga_id: str, state: dict[str, Any]) -> None:
        """Persist the state for a saga.

        Each top-level key in ``state`` is stored as a separate hash field
        with its value JSON-serialised.

        Args:
            saga_id: Unique identifier for the saga instance.
            state: Arbitrary serialisable state dict.
        """
        serialised = {k: json.dumps(v) for k, v in state.items()}
        await self._client.hset(self._key(saga_id), mapping=serialised)

    async def load(self, saga_id: str) -> dict[str, Any] | None:
        """Load persisted state for a saga.

        Args:
            saga_id: Unique identifier for the saga instance.

        Returns:
            The saved state dict with values deserialised, or None if the
            saga has no persisted state.
        """
        raw: dict[bytes, bytes] = await self._client.hgetall(self._key(saga_id))
        if not raw:
            return None
        return {
            (k.decode() if isinstance(k, bytes) else k): json.loads(v)
            for k, v in raw.items()
        }

    async def delete(self, saga_id: str) -> None:
        """Delete all persisted state for a saga.

        Args:
            saga_id: Unique identifier for the saga instance.
        """
        await self._client.delete(self._key(saga_id))


class RedisIdempotencyStore(IdempotencyStore):
    """Idempotency tracking backed by Redis string keys.

    Uses atomic SET NX EX so concurrent consumers can safely race to claim
    a message; only one will succeed.

    Example:
        import redis.asyncio as aioredis
        from sagakit.storage import RedisIdempotencyStore

        client = aioredis.from_url("redis://localhost:6379")
        store = RedisIdempotencyStore(client)
        claimed = await store.set_processing("msg-id-abc", ttl=3600)
        if not claimed:
            return  # duplicate — skip processing
    """

    def __init__(
        self, client: aioredis.Redis, default_ttl: int = _DEFAULT_TTL  # type: ignore[type-arg]
    ) -> None:
        self._client = client
        self._default_ttl = default_ttl

    def _key(self, key: str) -> str:
        return f"{_IDEMPOTENCY_KEY_PREFIX}{key}"

    async def set_processing(self, key: str, ttl: int) -> bool:
        """Atomically claim a key for processing (SET NX EX).

        Args:
            key: Idempotency key.
            ttl: Expiry in seconds.

        Returns:
            True if the key was claimed; False if it already existed.
        """
        result = await self._client.set(
            self._key(key), _STATUS_PROCESSING, nx=True, ex=ttl
        )
        return result is not None

    async def set_completed(self, key: str) -> None:
        """Mark a key as completed, preserving the default TTL.

        Args:
            key: Idempotency key.
        """
        await self._client.set(self._key(key), _STATUS_COMPLETED, ex=self._default_ttl)

    async def set_failed(self, key: str) -> None:
        """Mark a key as failed, preserving the default TTL.

        Args:
            key: Idempotency key.
        """
        await self._client.set(self._key(key), _STATUS_FAILED, ex=self._default_ttl)

    async def get_status(self, key: str) -> str | None:
        """Return the current status for a key, or None if it has expired.

        Args:
            key: Idempotency key.

        Returns:
            Status string or None.
        """
        value: bytes | None = await self._client.get(self._key(key))
        if value is None:
            return None
        return value.decode() if isinstance(value, bytes) else value
