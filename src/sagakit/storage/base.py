from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class StateStore(ABC):
    """Abstract base class for persisting saga state.

    Implementations must be safe to call concurrently from async tasks.

    Example:
        class MyStateStore(StateStore):
            async def save(self, saga_id: str, state: dict[str, Any]) -> None:
                ...
    """

    @abstractmethod
    async def save(self, saga_id: str, state: dict[str, Any]) -> None:
        """Persist the state for a saga.

        Args:
            saga_id: Unique identifier for the saga instance.
            state: Arbitrary serialisable state dict.
        """

    @abstractmethod
    async def load(self, saga_id: str) -> dict[str, Any] | None:
        """Load persisted state for a saga.

        Args:
            saga_id: Unique identifier for the saga instance.

        Returns:
            The saved state dict, or None if no state exists.
        """

    @abstractmethod
    async def delete(self, saga_id: str) -> None:
        """Delete all persisted state for a saga.

        Args:
            saga_id: Unique identifier for the saga instance.
        """


class IdempotencyStore(ABC):
    """Abstract base class for tracking message processing status.

    Provides atomic check-and-set semantics to prevent duplicate processing
    of the same message across concurrent consumers.

    Example:
        class MyIdempotencyStore(IdempotencyStore):
            async def set_processing(self, key: str, ttl: int) -> bool:
                ...
    """

    @abstractmethod
    async def set_processing(self, key: str, ttl: int) -> bool:
        """Atomically claim a key for processing if not already claimed.

        Uses SET NX EX semantics: sets the key only if it does not exist,
        with an expiry of ``ttl`` seconds.

        Args:
            key: Idempotency key (e.g. message ID or saga step key).
            ttl: Time-to-live in seconds.

        Returns:
            True if the key was successfully set (first time seen).
            False if the key already existed (duplicate).
        """

    @abstractmethod
    async def set_completed(self, key: str) -> None:
        """Mark a key as successfully completed.

        Args:
            key: Idempotency key to mark completed.
        """

    @abstractmethod
    async def set_failed(self, key: str) -> None:
        """Mark a key as failed.

        Args:
            key: Idempotency key to mark failed.
        """

    @abstractmethod
    async def get_status(self, key: str) -> str | None:
        """Return the current processing status for a key.

        Args:
            key: Idempotency key to query.

        Returns:
            The status string (e.g. "processing", "completed", "failed"),
            or None if the key does not exist.
        """
