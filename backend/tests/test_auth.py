"""
Auth, admin-gating, rate-limiting, and entity-id validation.

These guard the security-sensitive paths (live-mode bypass, admin dashboard,
per-user rate limit, SQL-interpolated entity ids). `_execute_sql` and the SDK
are stubbed (see conftest), so no real workspace is touched.
"""
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient


@pytest.fixture
def app_client(mock_execute_sql, monkeypatch):
    # Ensure the local-dev admin override is OFF so "no token" => non-admin.
    monkeypatch.delenv("LOCAL_DEV_ADMIN_EMAIL", raising=False)
    mock_execute_sql.register("SHOW CATALOGS", [{"catalog": "main"}])
    from backend.main import app
    return TestClient(app)


def test_user_info_without_token_is_non_admin(app_client):
    r = app_client.get("/api/user-info")
    assert r.status_code == 200
    assert r.json()["isAdmin"] is False


def test_admin_status_denied_without_admin(app_client):
    assert app_client.get("/api/admin/status").status_code == 403


def test_cache_invalidate_requires_admin(app_client):
    # Regression: this used to be IP-gated and failed open behind the Apps proxy.
    assert app_client.post("/api/cache/invalidate").status_code == 403


def test_admin_evict_cache_requires_admin(app_client):
    assert app_client.post("/api/admin/evict-cache?key=foo").status_code == 403


def test_rate_limit_returns_429(app_client):
    # Use a dedicated token so this test's bucket can't pollute (or be polluted by)
    # other tests. /api/catalogs needs no auth, so this exercises only the limiter.
    headers = {"x-forwarded-access-token": "rate-limit-bucket-token"}
    statuses = [app_client.get("/api/catalogs", headers=headers).status_code for _ in range(70)]
    assert statuses[0] == 200
    assert 429 in statuses  # default cap is 60/min


@pytest.mark.parametrize("entity_type", ["JOB", "PIPELINE", "NOTEBOOK", "DASHBOARD_V3"])
def test_validate_entity_id_rejects_injection(entity_type):
    from backend.main import _validate_entity_id
    with pytest.raises(HTTPException):
        _validate_entity_id(entity_type, "1; DROP TABLE x --")


def test_validate_entity_id_accepts_valid():
    from backend.main import _validate_entity_id
    assert _validate_entity_id("JOB", "12345") == "12345"


def test_resolve_entity_name_rejects_unsafe_id(mock_execute_sql):
    # The export path calls resolve_entity_name directly (no endpoint validation),
    # so the function must be injection-safe on its own: an unsafe id returns the
    # fallback name and never runs the interpolated query.
    from backend import lineage_service
    mock_execute_sql.register("system.lakeflow.jobs", [{"name": "SHOULD_NOT_BE_USED"}])
    out = lineage_service.resolve_entity_name("JOB", "1';DROP TABLE x--")
    assert out["name"].startswith("JOB ")
    assert out["name"] != "SHOULD_NOT_BE_USED"
