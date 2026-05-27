from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Message:
    """A message received from or published to a transport stream.

    Example:
        msg = Message(
            id="1700000000000-0",
            stream="orders",
            payload={"order_id": "abc123"},
            attributes={"requeue_count": "0"},
        )
    """

    id: str
    stream: str
    payload: dict[str, Any]
    attributes: dict[str, str]
