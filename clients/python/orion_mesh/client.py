"""Synchronous OrionMesh client.

REST calls are blocking via `requests`. The queue surface is async (via
`nats-py`) because the upstream client is async-only — the sync `Client`
proxies through a managed event loop so callers don't have to think
about it.
"""
from __future__ import annotations

import asyncio
import json
import os
import threading
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Union

import requests
import yaml

from .errors import ApplyFailed, DispatchFailed, OrionError, QueueNotFound, ResourceNotFound
from .models import Resource


def _default_controller() -> str:
    return os.environ.get("ORION_CONTROLLER_URL", "http://127.0.0.1:7878")


def _default_nats() -> str:
    return os.environ.get("NATS_URL", "nats://127.0.0.1:4222")


def _default_token() -> Optional[str]:
    return os.environ.get("ORION_CLUSTER_TOKEN") or None


class Client:
    """High-level OrionMesh client. All public methods are blocking.

    Parameters
    ----------
    controller : str
        Controller base URL. Defaults to `$ORION_CONTROLLER_URL` or
        `http://127.0.0.1:7878`.
    nats_url : str
        NATS broker URL. Defaults to `$NATS_URL` or `nats://127.0.0.1:4222`.
    token : str | None
        Cluster shared token. Defaults to `$ORION_CLUSTER_TOKEN`.
    timeout : float
        Per-request HTTP timeout (seconds). Default 10.
    """

    def __init__(
        self,
        controller: Optional[str] = None,
        nats_url: Optional[str] = None,
        token: Optional[str] = None,
        timeout: float = 10.0,
    ) -> None:
        self.controller = (controller or _default_controller()).rstrip("/")
        self.nats_url = nats_url or _default_nats()
        self.token = token or _default_token()
        self.timeout = timeout
        self._session = requests.Session()
        if self.token:
            self._session.headers["Authorization"] = f"Bearer {self.token}"
        # Lazy-initialised — only spun up when a queue method is called.
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._nats_conn = None
        self._js = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ REST

    def health(self) -> bool:
        try:
            r = self._session.get(f"{self.controller}/health", timeout=self.timeout)
            return r.ok
        except requests.RequestException:
            return False

    def get(self, kind: str, name: str) -> Resource:
        kind = _canonical_kind(kind)
        r = self._session.get(
            f"{self.controller}/v1/resources/{kind}/{name}", timeout=self.timeout
        )
        if r.status_code == 404:
            raise ResourceNotFound(kind, name)
        r.raise_for_status()
        return Resource.from_json(r.json())

    def list(self, kind: str) -> List[Resource]:
        kind = _canonical_kind(kind)
        r = self._session.get(
            f"{self.controller}/v1/resources/{kind}", timeout=self.timeout
        )
        r.raise_for_status()
        return [Resource.from_json(b) for b in r.json()]

    def apply(self, body: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Apply a resource document. Accepts a YAML string or a dict."""
        if isinstance(body, dict):
            body = yaml.safe_dump(body, sort_keys=False)
        r = self._session.post(
            f"{self.controller}/v1/resources/apply",
            data=body,
            headers={"content-type": "application/yaml"},
            timeout=self.timeout,
        )
        if not r.ok:
            raise ApplyFailed(r.status_code, r.text)
        return r.json()

    apply_yaml = apply  # alias for clarity in callers that always pass strings

    def delete(self, kind: str, name: str) -> bool:
        kind = _canonical_kind(kind)
        r = self._session.delete(
            f"{self.controller}/v1/resources/{kind}/{name}", timeout=self.timeout
        )
        if r.status_code == 404:
            raise ResourceNotFound(kind, name)
        r.raise_for_status()
        return r.json().get("deleted", False)

    def dispatch(self, kind: str, name: str) -> Dict[str, Any]:
        kind = _canonical_kind(kind)
        r = self._session.post(
            f"{self.controller}/v1/dispatch/{kind}/{name}", timeout=self.timeout
        )
        if not r.ok:
            raise DispatchFailed(f"dispatch {kind}/{name}: {r.status_code} {r.text}")
        return r.json()

    def logs(self, kind: str, name: str, since: int = 0) -> Dict[str, Any]:
        kind = _canonical_kind(kind)
        r = self._session.get(
            f"{self.controller}/v1/logs/{kind}/{name}",
            params={"since": since},
            timeout=self.timeout,
        )
        r.raise_for_status()
        return r.json()

    def find(self, selector: Dict[str, Any]) -> List[Resource]:
        """POST /v1/find with a capability selector. See docs/queues.md."""
        r = self._session.post(
            f"{self.controller}/v1/find",
            json=selector,
            timeout=self.timeout,
        )
        r.raise_for_status()
        return [Resource.from_json(b) for b in r.json()]

    def doctor(self) -> Dict[str, Any]:
        """Return the /v1/diag/system snapshot — same data the CLI's `orion doctor` shows."""
        r = self._session.get(
            f"{self.controller}/v1/diag/system", timeout=self.timeout
        )
        r.raise_for_status()
        return r.json()

    # ---------------------------------------------------------------- queues

    def queue(self, name: str) -> "Queue":
        return Queue(self, name)

    # ---------------------------------------------------------------- shutdown

    def close(self) -> None:
        """Tear down the NATS connection + helper loop. Safe to call repeatedly."""
        with self._lock:
            loop = self._loop
            self._loop = None
        if loop is not None:
            try:
                async def _bye():
                    if self._nats_conn is not None:
                        await self._nats_conn.close()

                future = asyncio.run_coroutine_threadsafe(_bye(), loop)
                future.result(timeout=2.0)
            except Exception:
                pass
            loop.call_soon_threadsafe(loop.stop)
            if self._loop_thread is not None:
                self._loop_thread.join(timeout=2.0)
        self._session.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---------------------------------------------------------------- internal

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._loop is not None:
                return self._loop
            self._loop = asyncio.new_event_loop()
            self._loop_thread = threading.Thread(
                target=self._loop.run_forever, daemon=True, name="orion-mesh-asyncio"
            )
            self._loop_thread.start()
            return self._loop

    def _run_async(self, coro):
        loop = self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()

    async def _nats(self):
        """Lazy NATS + JetStream init."""
        if self._js is not None:
            return self._nats_conn, self._js
        import nats

        opts = {"servers": [self.nats_url]}
        if self.token:
            opts["token"] = self.token
        self._nats_conn = await nats.connect(**opts)
        self._js = self._nats_conn.jetstream()
        return self._nats_conn, self._js


# ----------------------------------------------------------------- Queue API


class Queue:
    """Pub/sub helper for one named queue.

    Obtain via `client.queue(name)`. The queue is assumed to already
    exist as a Resource — call `client.apply(...)` first if you need to
    declare it.
    """

    def __init__(self, client: Client, name: str) -> None:
        self.client = client
        self.name = name
        # Cached spec — looked up on first use; refresh via `.refresh()`.
        self._spec: Optional[Dict[str, Any]] = None

    def refresh(self) -> Dict[str, Any]:
        try:
            r = self.client.get("Queue", self.name)
        except ResourceNotFound:
            raise QueueNotFound(self.name) from None
        self._spec = r.spec or {}
        return self._spec

    @property
    def spec(self) -> Dict[str, Any]:
        if self._spec is None:
            self.refresh()
        return self._spec  # type: ignore[return-value]

    @property
    def subject(self) -> str:
        s = self.spec.get("subject")
        return s or f"orion.queue.{self.name}"

    @property
    def stream(self) -> str:
        s = self.spec.get("stream")
        return s or "ORION_QUEUE_" + self.name.upper().replace("-", "_")

    @property
    def type(self) -> str:
        return self.spec.get("type", "work")

    # -- publishing -----------------------------------------------------

    def pub(self, value: Union[Dict[str, Any], str, bytes]) -> int:
        """Publish one message. Returns the JetStream sequence number."""
        async def _do():
            _, js = await self.client._nats()
            await self._ensure_stream(js)
            payload = _to_bytes(value)
            ack = await js.publish(self.subject, payload)
            return ack.seq

        return self.client._run_async(_do())

    def pub_many(self, values: Iterable[Union[Dict[str, Any], str, bytes]]) -> int:
        """Publish a batch; returns the count."""
        async def _do():
            _, js = await self.client._nats()
            await self._ensure_stream(js)
            count = 0
            for v in values:
                await js.publish(self.subject, _to_bytes(v))
                count += 1
            return count

        return self.client._run_async(_do())

    # -- subscribing ----------------------------------------------------

    def sub(
        self,
        group: Optional[str] = None,
        limit: Optional[int] = None,
        ack: bool = True,
        fetch_timeout: float = 5.0,
    ) -> Iterator[Dict[str, Any]]:
        """Subscribe and yield decoded rows.

        Parameters
        ----------
        group : str, optional
            JetStream durable consumer name. For `work` queues, sharing this
            across subscribers load-balances; for `topic` queues, omit (or
            pick a unique name) so each subscriber gets every message.
        limit : int, optional
            Stop after this many messages. None = run forever.
        ack : bool
            Ack each message after the handler returns. Set False for
            non-destructive inspection of a `work` queue.
        """
        spec = self.spec
        qtype = spec.get("type", "work")
        if group is None:
            if qtype == "work":
                group = f"{self.name}-py-workers"
            else:
                # Unique-per-process for topic so we get every message.
                group = f"{self.name}-py-{os.getpid()}"

        # The blocking iterator pulls one batch at a time via the worker loop.
        loop = self.client._ensure_loop()

        async def _setup():
            from nats.js.api import AckPolicy, ConsumerConfig, StreamConfig
            from nats.js.errors import NotFoundError

            _, js = await self.client._nats()
            try:
                await js.stream_info(self.stream)
            except NotFoundError:
                await js.add_stream(StreamConfig(name=self.stream, subjects=[self.subject]))
            consumer = await js.pull_subscribe(
                subject=self.subject,
                durable=group,
                stream=self.stream,
                config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT),
            )
            return consumer

        consumer = asyncio.run_coroutine_threadsafe(_setup(), loop).result()

        delivered = 0
        while True:
            if limit is not None and delivered >= limit:
                return

            async def _fetch(c=consumer):
                try:
                    msgs = await c.fetch(batch=1, timeout=fetch_timeout)
                except asyncio.TimeoutError:
                    return []
                return msgs

            msgs = asyncio.run_coroutine_threadsafe(_fetch(), loop).result()
            if not msgs:
                if limit is None:
                    continue
                # Honour `limit` even when the queue is dry by stopping after
                # one empty fetch. Callers that want to wait should set
                # limit=None.
                return
            for m in msgs:
                try:
                    text = m.data.decode("utf-8", errors="replace")
                except Exception:
                    text = m.data.hex()
                try:
                    row = json.loads(text)
                except json.JSONDecodeError:
                    row = {"_raw": text}
                if isinstance(row, dict):
                    row.setdefault("_subject", m.subject)
                yield row
                if ack:
                    asyncio.run_coroutine_threadsafe(m.ack(), loop).result()
                delivered += 1
                if limit is not None and delivered >= limit:
                    return

    def consume(
        self,
        handler: Callable[[Dict[str, Any]], None],
        group: Optional[str] = None,
        ack: bool = True,
    ) -> None:
        """Convenience: run `handler(row)` for every message forever."""
        for row in self.sub(group=group, ack=ack):
            handler(row)

    # -- internals ------------------------------------------------------

    async def _ensure_stream(self, js) -> None:
        """Idempotent get-or-create matching what the Rust CLI does."""
        from nats.js.api import StreamConfig
        from nats.js.errors import NotFoundError

        try:
            await js.stream_info(self.stream)
        except NotFoundError:
            await js.add_stream(StreamConfig(name=self.stream, subjects=[self.subject]))


# ----------------------------------------------------------------- helpers


def _canonical_kind(s: str) -> str:
    """Mirrors the CLI's `util::canonical_kind` — `services` and `Service` both
    normalise to "Service". Keeps caller-side ergonomics consistent."""
    s = s.rstrip("s") if not s.endswith("ss") else s
    return s[:1].upper() + s[1:]


def _to_bytes(v: Union[Dict[str, Any], str, bytes]) -> bytes:
    if isinstance(v, bytes):
        return v
    if isinstance(v, str):
        return v.encode("utf-8")
    return json.dumps(v, sort_keys=True).encode("utf-8")


@contextmanager
def _suppress(exc_type):
    try:
        yield
    except exc_type:
        pass
