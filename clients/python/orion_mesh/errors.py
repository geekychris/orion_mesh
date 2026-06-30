"""Error types raised by the OrionMesh client."""
from __future__ import annotations


class OrionError(Exception):
    """Base class for every error this client raises."""


class ResourceNotFound(OrionError):
    """Raised by `Client.get` / `Client.delete` when the controller returns 404."""

    def __init__(self, kind: str, name: str):
        super().__init__(f"{kind}/{name} not found")
        self.kind = kind
        self.name = name


class ApplyFailed(OrionError):
    """Raised when POST /v1/resources/apply returns a 4xx/5xx with detail."""

    def __init__(self, status: int, detail: str):
        super().__init__(f"apply failed ({status}): {detail}")
        self.status = status
        self.detail = detail


class DispatchFailed(OrionError):
    """Raised when POST /v1/dispatch fails (e.g. no live nodes match placement)."""


class QueueNotFound(OrionError):
    """Raised when publishing or subscribing to a queue that doesn't exist."""

    def __init__(self, name: str):
        super().__init__(f"queue {name!r} not declared — apply a Queue resource first")
        self.name = name
