"""Unit tests for the REST surface. Mocked via `responses` — no broker
required."""
from __future__ import annotations

import pytest
import responses

from orion_mesh import Client, OrionError, Resource
from orion_mesh.errors import ApplyFailed, DispatchFailed, ResourceNotFound


@pytest.fixture
def client():
    return Client(controller="http://orion-test.local", token="tkn")


# --------------------------------------------------------------- token + URL


def test_picks_up_env_when_no_args(monkeypatch):
    monkeypatch.setenv("ORION_CONTROLLER_URL", "http://envhost:9999")
    monkeypatch.setenv("ORION_CLUSTER_TOKEN", "envtoken")
    c = Client()
    assert c.controller == "http://envhost:9999"
    assert c.token == "envtoken"
    assert c._session.headers["Authorization"] == "Bearer envtoken"


def test_trailing_slash_in_controller_url_is_stripped():
    c = Client(controller="http://x.local/")
    assert c.controller == "http://x.local"


# ----------------------------------------------------------------- health


@responses.activate
def test_health_returns_true_on_2xx(client):
    responses.add(responses.GET, "http://orion-test.local/health", body="ok", status=200)
    assert client.health() is True


@responses.activate
def test_health_returns_false_on_error(client):
    responses.add(responses.GET, "http://orion-test.local/health", status=500)
    assert client.health() is False


# ----------------------------------------------------------------- get/list


@responses.activate
def test_get_returns_typed_resource(client):
    responses.add(
        responses.GET,
        "http://orion-test.local/v1/resources/Service/web",
        json={"apiVersion": "orionmesh.dev/v1", "kind": "Service", "metadata": {"name": "web"}, "spec": {"replicas": 2}},
    )
    r = client.get("Service", "web")
    assert isinstance(r, Resource)
    assert r.kind == "Service"
    assert r.name == "web"
    assert r.spec == {"replicas": 2}


@responses.activate
def test_get_accepts_plural_lowercase_kind(client):
    responses.add(
        responses.GET,
        "http://orion-test.local/v1/resources/Service/web",
        json={"kind": "Service", "metadata": {"name": "web"}, "spec": {}},
    )
    assert client.get("services", "web").name == "web"


@responses.activate
def test_get_raises_resource_not_found_on_404(client):
    responses.add(
        responses.GET, "http://orion-test.local/v1/resources/Service/missing", status=404
    )
    with pytest.raises(ResourceNotFound) as ei:
        client.get("Service", "missing")
    assert ei.value.kind == "Service"
    assert ei.value.name == "missing"


@responses.activate
def test_list_returns_all_matching(client):
    responses.add(
        responses.GET,
        "http://orion-test.local/v1/resources/Service",
        json=[
            {"kind": "Service", "metadata": {"name": "a"}, "spec": {}},
            {"kind": "Service", "metadata": {"name": "b"}, "spec": {}},
        ],
    )
    rs = client.list("Service")
    assert [r.name for r in rs] == ["a", "b"]


# ----------------------------------------------------------------- apply


@responses.activate
def test_apply_accepts_yaml_string(client):
    responses.add(
        responses.POST,
        "http://orion-test.local/v1/resources/apply",
        json={"applied": True, "kind": "Service", "name": "x", "generation": 1},
    )
    out = client.apply("kind: Service\nmetadata:\n  name: x\nspec: {}\n")
    assert out["applied"] is True
    assert out["generation"] == 1


@responses.activate
def test_apply_accepts_dict(client):
    responses.add(
        responses.POST,
        "http://orion-test.local/v1/resources/apply",
        json={"applied": True},
    )
    out = client.apply({"apiVersion": "orionmesh.dev/v1", "kind": "Queue", "metadata": {"name": "q"}, "spec": {"type": "work"}})
    assert out == {"applied": True}


@responses.activate
def test_apply_raises_apply_failed_on_4xx_with_detail(client):
    responses.add(
        responses.POST,
        "http://orion-test.local/v1/resources/apply",
        body="invalid yaml at line 3",
        status=400,
    )
    with pytest.raises(ApplyFailed) as ei:
        client.apply("kind: bogus\n")
    assert ei.value.status == 400
    assert "line 3" in ei.value.detail


# ----------------------------------------------------------------- delete


@responses.activate
def test_delete_returns_bool(client):
    responses.add(
        responses.DELETE,
        "http://orion-test.local/v1/resources/Service/web",
        json={"deleted": True, "kind": "Service", "name": "web"},
    )
    assert client.delete("Service", "web") is True


@responses.activate
def test_delete_raises_resource_not_found_on_404(client):
    responses.add(
        responses.DELETE,
        "http://orion-test.local/v1/resources/Service/web",
        status=404,
    )
    with pytest.raises(ResourceNotFound):
        client.delete("Service", "web")


# ----------------------------------------------------------------- dispatch


@responses.activate
def test_dispatch_returns_response_json(client):
    responses.add(
        responses.POST,
        "http://orion-test.local/v1/dispatch/Service/web",
        json={"instance_id": "00000000-0000-0000-0000-000000000001", "node": "n1"},
    )
    out = client.dispatch("Service", "web")
    assert out["node"] == "n1"


@responses.activate
def test_dispatch_raises_dispatch_failed_on_bad_status(client):
    responses.add(
        responses.POST,
        "http://orion-test.local/v1/dispatch/Service/web",
        body="no live nodes",
        status=400,
    )
    with pytest.raises(DispatchFailed):
        client.dispatch("Service", "web")


# ----------------------------------------------------------------- logs


@responses.activate
def test_logs_passes_since_param(client):
    responses.add(
        responses.GET,
        "http://orion-test.local/v1/logs/Service/web",
        json={"total": 5, "entries": []},
    )
    out = client.logs("Service", "web", since=12)
    assert out["total"] == 5
    assert responses.calls[0].request.url.endswith("?since=12")


# ----------------------------------------------------------------- find


@responses.activate
def test_find_posts_selector(client):
    responses.add(
        responses.POST,
        "http://orion-test.local/v1/find",
        json=[{"kind": "Service", "metadata": {"name": "llm"}, "spec": {"capabilities": []}}],
    )
    out = client.find({"llm": {"min_vram_gb": {"gte": 24}}})
    assert len(out) == 1
    assert out[0].name == "llm"
    body = responses.calls[0].request.body
    assert b"llm" in body
    assert b"gte" in body


# ----------------------------------------------------------------- doctor


@responses.activate
def test_doctor_passes_through(client):
    responses.add(
        responses.GET,
        "http://orion-test.local/v1/diag/system",
        json={"agents": 1, "instances": {"total": 0}},
    )
    assert client.doctor()["agents"] == 1


# ----------------------------------------------------------------- context mgr


def test_context_manager_calls_close():
    c = Client(controller="http://x")
    with c as same:
        assert same is c
    # close() doesn't crash even when no loop was started.


# ----------------------------------------------------------------- Queue surface


@responses.activate
def test_queue_helper_resolves_subject_and_stream_from_resource(client):
    # First call returns the Queue spec.
    responses.add(
        responses.GET,
        "http://orion-test.local/v1/resources/Queue/orders",
        json={
            "kind": "Queue",
            "metadata": {"name": "orders"},
            "spec": {"type": "work", "max_age_seconds": 3600},
        },
    )
    q = client.queue("orders")
    assert q.subject == "orion.queue.orders"
    assert q.stream == "ORION_QUEUE_ORDERS"
    assert q.type == "work"


@responses.activate
def test_queue_default_subject_when_override_present(client):
    responses.add(
        responses.GET,
        "http://orion-test.local/v1/resources/Queue/orders",
        json={
            "kind": "Queue",
            "metadata": {"name": "orders"},
            "spec": {"type": "topic", "subject": "custom.subject"},
        },
    )
    q = client.queue("orders")
    assert q.subject == "custom.subject"
    assert q.type == "topic"


@responses.activate
def test_queue_refresh_raises_queue_not_found_on_404(client):
    from orion_mesh.errors import QueueNotFound

    responses.add(
        responses.GET,
        "http://orion-test.local/v1/resources/Queue/missing",
        status=404,
    )
    q = client.queue("missing")
    with pytest.raises(QueueNotFound):
        q.refresh()
