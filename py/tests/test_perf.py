from __future__ import annotations

import json
import time
from datetime import date, timedelta
from pathlib import Path

from fastapi.testclient import TestClient
from gh_trending_analytics.build import build_kind
from gh_trending_analytics.query import DuckDBQueryService, QueryConfig
from gh_trending_web.app import create_app


def _write_archive(root: Path, start: date, days: int) -> None:
    languages = ["python", "c++", None]
    for offset in range(days):
        current = start + timedelta(days=offset)
        day_str = current.isoformat()
        year = str(current.year)
        for kind in ["repository", "developer"]:
            for language in languages:
                dir_path = root / kind / year / day_str
                dir_path.mkdir(parents=True, exist_ok=True)
                filename = f"{language}.json" if language is not None else "(null).json"
                items = [f"item{idx}" for idx in range(10)]
                if kind == "repository":
                    entries = [f"owner{idx}/{items[idx]}" for idx in range(10)]
                else:
                    entries = [f"user{idx}" for idx in range(10)]
                payload = {"date": day_str, "language": language, "list": entries}
                (dir_path / filename).write_text(json.dumps(payload))


def test_perf_cached_day_and_toplists(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    _write_archive(archive_root, date(2025, 1, 1), 90)

    analytics_root = tmp_path / "analytics"
    build_kind(
        archive_root=archive_root,
        analytics_root=analytics_root,
        kind="repository",
        rebuild_year=True,
    )
    build_kind(
        archive_root=archive_root,
        analytics_root=analytics_root,
        kind="developer",
        rebuild_year=True,
    )

    app = create_app(analytics_root=analytics_root)
    client = TestClient(app)

    params = {"kind": "repository", "date": "2025-01-15", "language": "python"}
    client.get("/api/v1/day", params=params)
    start = time.perf_counter()
    response = client.get("/api/v1/day", params=params)
    elapsed = time.perf_counter() - start
    assert response.status_code == 200
    assert elapsed < 0.5, f"cached day response took {elapsed:.3f}s"

    query_service = DuckDBQueryService(QueryConfig(analytics_root=analytics_root))
    start = time.perf_counter()
    query_service.top_reappearing(
        "repository",
        "2025-01-01",
        "2025-01-30",
        language=None,
        presence="day",
        include_all_languages=False,
        limit=10,
    )
    elapsed_30 = time.perf_counter() - start

    start = time.perf_counter()
    query_service.top_reappearing(
        "repository",
        "2025-01-01",
        "2025-03-31",
        language=None,
        presence="day",
        include_all_languages=False,
        limit=10,
    )
    elapsed_90 = time.perf_counter() - start

    assert elapsed_30 < 0.75, f"top_reappearing 30 days took {elapsed_30:.3f}s"
    assert elapsed_90 < 1.0, f"top_reappearing 90 days took {elapsed_90:.3f}s"
