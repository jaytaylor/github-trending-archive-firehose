from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient
from gh_trending_web.app import create_app
from helpers import build_fixture


def _client(tmp_path: Path) -> TestClient:
    analytics_root = build_fixture(tmp_path)
    app = create_app(analytics_root=analytics_root)
    return TestClient(app)


def _decode_key(key: str) -> dict:
    _, payload = key.split(":", 1)
    return json.loads(payload)


def test_prewarm_skips_out_of_range(tmp_path: Path) -> None:
    client = _client(tmp_path)
    response = client.get(
        "/api/v1/day",
        params={"kind": "repository", "date": "2025-01-01", "language": "python"},
    )
    assert response.status_code == 200
    cache = client.app.state.cache
    keys = [_decode_key(key) for key in cache.keys()]
    dates = {item["date"] for item in keys if item.get("kind") == "repository"}
    assert dates.issubset({"2025-01-01", "2025-01-02"})


def test_prewarm_failure_safe(tmp_path: Path) -> None:
    analytics_root = build_fixture(tmp_path)
    app = create_app(analytics_root=analytics_root)

    original_get_day = app.state.query_service.get_day

    def guarded_get_day(kind: str, date: str, language: str):
        if date == "2025-01-02":
            raise RuntimeError("boom")
        return original_get_day(kind, date, language)

    app.state.query_service.get_day = guarded_get_day  # type: ignore[assignment]
    client = TestClient(app)
    response = client.get(
        "/api/v1/day",
        params={"kind": "repository", "date": "2025-01-01", "language": "python"},
    )
    assert response.status_code == 200
    assert app.state.cache.stats.prewarm_failure >= 1
