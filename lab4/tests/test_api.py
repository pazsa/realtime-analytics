"""API tests that need no ClickHouse.

TestClient is used without `with`, so the app lifespan (which connects to
ClickHouse) is not started. Every test here is answered before any query runs:
request validation, /ping, /metrics, and a cache hit.
"""

import pytest
from fastapi.testclient import TestClient

import app as app_module
from cache import TTLCache


@pytest.fixture
def client():
    return TestClient(app_module.app)


def test_ping(client):
    r = client.get("/ping")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_metrics_endpoint_exposes_prometheus_text(client):
    r = client.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    assert "lab04_cache_requests_total" in r.text


def test_dataset_id_must_match_pattern(client):
    assert client.get("/v1/datasets/BAD_ID/orders").status_code == 422


def test_orders_limit_must_be_in_range(client):
    assert client.get("/v1/datasets/ds1/orders", params={"limit": 0}).status_code == 422
    assert client.get("/v1/datasets/ds1/orders", params={"limit": 101}).status_code == 422


def test_orders_rejects_unknown_query_parameters(client):
    assert client.get("/v1/datasets/ds1/orders", params={"foo": "1"}).status_code == 422


def test_timeseries_range_must_be_on_the_hour(client):
    params = {"from": "2026-01-01T00:30:00Z", "to": "2026-01-01T03:00:00Z"}
    assert client.get("/v1/datasets/ds1/metrics/timeseries", params=params).status_code == 422


def test_timeseries_range_must_be_ordered(client):
    params = {"from": "2026-01-01T05:00:00Z", "to": "2026-01-01T03:00:00Z"}
    assert client.get("/v1/datasets/ds1/metrics/timeseries", params=params).status_code == 422


def test_timeseries_range_is_limited_to_31_days(client):
    params = {"from": "2026-01-01T00:00:00Z", "to": "2026-03-01T00:00:00Z"}
    assert client.get("/v1/datasets/ds1/metrics/timeseries", params=params).status_code == 422


def test_summary_is_served_from_cache(client, monkeypatch):
    cache = TTLCache(ttl=30, max_entries=8)
    cache.put(
        "ds1",
        app_module.SummaryMetric(
            data=app_module.SummaryAmount(paid_amount_cents=1234),
            meta=app_module.MetaData(computed_at="2026-01-01T00:00:00Z", latest_event_at=None),
        ),
    )
    monkeypatch.setattr(app_module.app.state, "cache", cache, raising=False)

    r = client.get("/v1/datasets/ds1/metrics/summary")
    assert r.status_code == 200
    assert r.json()["data"]["paid_amount_cents"] == 1234
