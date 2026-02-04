from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from gh_trending_web.app import create_app
from helpers import build_fixture


def _client(tmp_path: Path) -> TestClient:
    analytics_root = build_fixture(tmp_path)
    app = create_app(analytics_root=analytics_root)
    return TestClient(app)


def test_day_endpoint_ok(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get(
        "/api/v1/day", params={"kind": "repository", "date": "2025-01-01", "language": "python"}
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["kind"] == "repository"
    assert payload["date"] == "2025-01-01"
    assert payload["entries"][0]["rank"] == 1
    assert payload["entries"][0]["full_name"] == "alpha/one"


def test_invalid_date_returns_400(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get(
        "/api/v1/day",
        params={"kind": "repository", "date": "2025-13-01", "language": "python"},
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["error"] == "invalid_request"


def test_missing_date_returns_404(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get(
        "/api/v1/day",
        params={"kind": "repository", "date": "2025-01-05", "language": "python"},
    )
    assert response.status_code == 404
    payload = response.json()
    assert payload["error"] == "date_not_found"
    assert "Try one of" in payload.get("hint", "")


def test_invalid_kind_returns_400(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get("/api/v1/dates", params={"kind": "repos"})
    assert response.status_code == 400
    payload = response.json()
    assert payload["error"] == "invalid_kind"


def test_sql_injection_rejected(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get(
        "/api/v1/day",
        params={
            "kind": "repository",
            "date": "2025-01-01",
            "language": "python' OR 1=1 --",
        },
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["error"] == "invalid_request"


def test_invalid_language_returns_400(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get(
        "/api/v1/day",
        params={
            "kind": "repository",
            "date": "2025-01-01",
            "language": "not-a-language",
        },
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["error"] == "invalid_request"


def test_invalid_presence_returns_400(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get(
        "/api/v1/top/reappearing",
        params={
            "kind": "repository",
            "start": "2025-01-01",
            "end": "2025-01-02",
            "presence": "weeks",
        },
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["error"] == "invalid_request"


def test_invalid_include_all_languages_returns_400(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get(
        "/api/v1/top/owners",
        params={"start": "2025-01-01", "end": "2025-01-02", "include_all_languages": "maybe"},
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["error"] == "invalid_request"


def test_invalid_limit_returns_400(tmp_path: Path) -> None:
    client = _client(tmp_path)
    for limit in ["0", "501"]:
        response = client.get(
            "/api/v1/top/reappearing",
            params={
                "kind": "repository",
                "start": "2025-01-01",
                "end": "2025-01-02",
                "limit": limit,
            },
        )
        assert response.status_code == 400
        payload = response.json()
        assert payload["error"] == "invalid_request"


def test_non_int_limit_returns_400(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get(
        "/api/v1/top/newcomers",
        params={
            "kind": "repository",
            "start": "2025-01-01",
            "end": "2025-01-02",
            "limit": "ten",
        },
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["error"] == "invalid_request"


def test_invalid_range_reappearing_returns_400(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get(
        "/api/v1/top/reappearing",
        params={
            "kind": "repository",
            "start": "2025-01-02",
            "end": "2025-01-01",
        },
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["error"] == "invalid_request"


def test_invalid_range_returns_400(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get(
        "/api/v1/top/streaks",
        params={"kind": "repository", "start": "2025-01-02", "end": "2025-01-01"},
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["error"] == "invalid_request"
