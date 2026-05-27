from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any

from sagakit.transport.message import Message


class Transport(ABC):
    """Abstract base class for saga message transports.

    Implementations must be async-safe and support consumer groups so that
    multiple workers can share a stream without duplicate processing.

    Example:
        class MyTransport(Transport):
            async def publish(self, stream: str, message: dict[str, Any]) -> str:
                ...
    """

    @abstractmethod
    async def publish(self, stream: str, message: dict[str, Any]) -> str:
        """Publish a message to a stream.

        Args:
            stream: The stream (topic) name.
            message: Arbitrary payload dict.

        Returns:
            The message ID assigned by the transport.
        """

    @abstractmethod
    def consume(self, stream: str, group: str, consumer: str) -> AsyncIterator[Message]:
        """Yield messages from a stream consumer group.

        Args:
            stream: The stream name.
            group: Consumer group name.
            consumer: Unique consumer identifier within the group.
        """

    @abstractmethod
    async def acknowledge(self, stream: str, group: str, message_id: str) -> None:
        """Acknowledge successful processing of a message.

        Args:
            stream: The stream name.
            group: Consumer group name.
            message_id: The message ID to acknowledge.
        """

    @abstractmethod
    async def reject(
        self, stream: str, group: str, message_id: str, requeue: bool = True
    ) -> None:
        """Reject a message, either requeueing or discarding it.

        Args:
            stream: The stream name.
            group: Consumer group name.
            message_id: The message ID to reject.
            requeue: If True, re-publish the message so any consumer can retry.
                     If False, move the message to the dead-letter stream.
        """
