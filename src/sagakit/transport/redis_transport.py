from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, cast

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from sagakit.transport.base import Transport
from sagakit.transport.message import Message

_STREAM_MAXLEN = 10_000
_CONSUME_BLOCK_MS = 2_000
_CONSUME_COUNT = 1

# redis-py's xadd expects this exact field dict type; we cast from dict[str, Any]
# at call sites because our public API cannot be that restrictive.
# memoryview[int] is only subscriptable at runtime on Python 3.12+, so keep
# this alias type-checker-only; cast() is a no-op at runtime regardless.
if TYPE_CHECKING:
    _XAddFields = dict[
        bytes | bytearray | memoryview[int] | str | int | float,
        bytes | bytearray | memoryview[int] | str | int | float,
    ]
else:
    _XAddFields = dict


class RedisStreamsTransport(Transport):
    """Transport implementation backed by Redis Streams consumer groups.

    The Redis client is injected rather than created internally so the caller
    controls connection pooling, TLS, and sentinel/cluster topology.

    Example:
        import redis.asyncio as aioredis
        from sagakit.transport import RedisStreamsTransport

        client = aioredis.from_url("redis://localhost:6379")
        transport = RedisStreamsTransport(client)
        await transport.initialize("orders", "saga-workers")
        msg_id = await transport.publish("orders", {"order_id": "abc"})
    """

    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def initialize(self, stream: str, group: str) -> None:
        """Create the consumer group (and stream) if they do not exist.

        Safe to call multiple times — silently ignores the error Redis raises
        when the group already exists.

        Args:
            stream: Stream name.
            group: Consumer group name.
        """
        try:
            await self._client.xgroup_create(stream, group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def publish(self, stream: str, message: dict[str, Any]) -> str:
        """Add a message to the stream, keeping the stream bounded.

        Args:
            stream: Stream name.
            message: Payload dict; values must be Redis-serialisable.

        Returns:
            The Redis entry ID assigned to the new message.
        """
        msg_id: str = await self._client.xadd(
            stream, cast(_XAddFields, message), maxlen=_STREAM_MAXLEN, approximate=True
        )
        return msg_id

    async def consume(self, stream: str, group: str, consumer: str) -> AsyncIterator[Message]:
        """Yield unacknowledged messages from the consumer group.

        Blocks for up to 2 seconds per poll when the stream is empty so the
        caller's event loop is not busy-waited. Loops forever; cancel the
        enclosing task to stop.

        Args:
            stream: Stream name.
            group: Consumer group name.
            consumer: Unique consumer name within the group.
        """
        while True:
            results = await self._client.xreadgroup(
                group,
                consumer,
                {stream: ">"},
                count=_CONSUME_COUNT,
                block=_CONSUME_BLOCK_MS,
            )
            if not results:
                continue
            for _stream, entries in results:
                for entry_id, fields in entries:
                    payload: dict[str, Any] = {}
                    attributes: dict[str, str] = {}
                    for k, v in fields.items():
                        key = k.decode() if isinstance(k, bytes) else k
                        val = v.decode() if isinstance(v, bytes) else v
                        if key.startswith("attr:"):
                            attributes[key[5:]] = val
                        else:
                            payload[key] = val
                    raw_id = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                    raw_stream = _stream.decode() if isinstance(_stream, bytes) else _stream
                    yield Message(
                        id=raw_id,
                        stream=raw_stream,
                        payload=payload,
                        attributes=attributes,
                    )

    async def acknowledge(self, stream: str, group: str, message_id: str) -> None:
        """Acknowledge that a message was processed successfully.

        Args:
            stream: Stream name.
            group: Consumer group name.
            message_id: Entry ID to acknowledge.
        """
        await self._client.xack(stream, group, message_id)

    async def reject(self, stream: str, group: str, message_id: str, requeue: bool = True) -> None:
        """Reject a message.

        When ``requeue=True`` the message is ACKed then re-published to the
        same stream so any consumer in the group can retry it.  The
        ``requeue_count`` attribute is incremented so callers can implement
        a max-retry limit.

        When ``requeue=False`` the message is moved to ``{stream}:dlq``
        before being ACKed, so it is never lost.

        Args:
            stream: Stream name.
            group: Consumer group name.
            message_id: Entry ID to reject.
            requeue: True to retry on the same stream; False for dead-letter.
        """
        # XRANGE with the exact ID returns at most one entry.
        entries = await self._client.xrange(stream, min=message_id, max=message_id)
        if entries:
            _id, fields = entries[0]
            payload: dict[str, Any] = {}
            attributes: dict[str, str] = {}
            for k, v in fields.items():
                key = k.decode() if isinstance(k, bytes) else k
                val = v.decode() if isinstance(v, bytes) else v
                if key.startswith("attr:"):
                    attributes[key[5:]] = val
                else:
                    payload[key] = val
        else:
            payload = {}
            attributes = {}

        if requeue:
            requeue_count = int(attributes.get("requeue_count", "0")) + 1
            attributes = {**attributes, "requeue_count": str(requeue_count)}
            republish: dict[str, Any] = {**payload}
            for attr_key, attr_val in attributes.items():
                republish[f"attr:{attr_key}"] = attr_val
            await self._client.xadd(
                stream, cast(_XAddFields, republish), maxlen=_STREAM_MAXLEN, approximate=True
            )
            await self._client.xack(stream, group, message_id)
        else:
            dlq_stream = f"{stream}:dlq"
            dlq_entry: dict[str, Any] = {**payload}
            for attr_key, attr_val in attributes.items():
                dlq_entry[f"attr:{attr_key}"] = attr_val
            dlq_entry["attr:original_id"] = message_id
            await self._client.xadd(dlq_stream, cast(_XAddFields, dlq_entry))
            await self._client.xack(stream, group, message_id)
