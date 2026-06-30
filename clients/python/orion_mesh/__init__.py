"""Python client for OrionMesh.

Quick start
-----------

    from orion_mesh import Client

    o = Client()                                  # picks up ORION_CONTROLLER_URL etc.
    o.apply_yaml("apiVersion: orionmesh.dev/v1\\nkind: Queue\\nmetadata: { name: events }\\nspec: { type: work }\\n")
    o.queue("events").pub({"hello": "world"})

    for row in o.queue("events").sub(group="readers", limit=10):
        print(row)

Module layout: `client.Client` is the high-level entrypoint; everything
else (`Queue`, `Resource`, `errors`) is reachable through it.
"""

from .client import Client, Queue
from .errors import OrionError, ResourceNotFound, ApplyFailed
from .models import Resource

__all__ = ["Client", "Queue", "Resource", "OrionError", "ResourceNotFound", "ApplyFailed"]
__version__ = "0.1.0"
