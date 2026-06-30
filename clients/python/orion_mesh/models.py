"""Lightweight typed views over the JSON the controller returns."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class Resource:
    """A Resource document as the controller stores it.

    Not a strict schema — the controller's wire format evolves, and the
    client deliberately stays flexible. Fields the client cares about are
    extracted from `body`; the original JSON sits on `raw` for code that
    needs the whole document.
    """

    kind: str
    name: str
    namespace: str = "_"
    api_version: str = "orionmesh.dev/v1"
    spec: Dict[str, Any] = field(default_factory=dict)
    status: Optional[Dict[str, Any]] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, body: Dict[str, Any]) -> "Resource":
        return cls(
            kind=body.get("kind", ""),
            name=body.get("metadata", {}).get("name", ""),
            namespace=body.get("metadata", {}).get("namespace", "_"),
            api_version=body.get("apiVersion", "orionmesh.dev/v1"),
            spec=body.get("spec", {}) or {},
            status=body.get("status"),
            raw=body,
        )
