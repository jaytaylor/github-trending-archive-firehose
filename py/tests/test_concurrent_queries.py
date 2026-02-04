from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from gh_trending_analytics.query import DuckDBQueryService, QueryConfig
from helpers import build_fixture


def _service(tmp_path: Path) -> DuckDBQueryService:
    analytics_root = build_fixture(tmp_path)
    return DuckDBQueryService(QueryConfig(analytics_root=analytics_root))


def test_concurrent_queries(tmp_path: Path) -> None:
    service = _service(tmp_path)

    def task() -> int:
        results = service.top_reappearing(
            "repository",
            "2025-01-01",
            "2025-01-02",
            language=None,
            presence="day",
            include_all_languages=True,
            limit=5,
        )
        return len(results)

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: task(), range(8)))

    assert all(count > 0 for count in results)
