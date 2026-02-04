from __future__ import annotations

from pathlib import Path

import pytest
from gh_trending_analytics.errors import InvalidRequestError, NotFoundError
from gh_trending_analytics.query import DuckDBQueryService, QueryConfig
from helpers import build_fixture


def _service(tmp_path: Path) -> DuckDBQueryService:
    analytics_root = build_fixture(tmp_path)
    return DuckDBQueryService(QueryConfig(analytics_root=analytics_root))


def test_presence_day_vs_occurrence(tmp_path: Path) -> None:
    service = _service(tmp_path)

    day_results = service.top_reappearing(
        "repository",
        "2025-01-01",
        "2025-01-02",
        language=None,
        presence="day",
        include_all_languages=True,
        limit=10,
    )
    occ_results = service.top_reappearing(
        "repository",
        "2025-01-01",
        "2025-01-02",
        language=None,
        presence="occurrence",
        include_all_languages=True,
        limit=10,
    )

    day_alpha = next(item for item in day_results if item["full_name"] == "alpha/one")
    occ_alpha = next(item for item in occ_results if item["full_name"] == "alpha/one")

    assert day_alpha["days_present"] == 2
    assert occ_alpha["days_present"] == 5


def test_get_day_all_languages(tmp_path: Path) -> None:
    service = _service(tmp_path)
    entries = service.get_day("repository", "2025-01-01", "__all__")
    assert [entry["full_name"] for entry in entries] == ["omega/all", "alpha/one"]


def test_invalid_date_format(tmp_path: Path) -> None:
    service = _service(tmp_path)
    with pytest.raises(InvalidRequestError):
        service.get_day("repository", "2025-13-01", "python")


def test_missing_date(tmp_path: Path) -> None:
    service = _service(tmp_path)
    with pytest.raises(NotFoundError):
        service.get_day("repository", "2025-01-05", "python")


def test_sql_injection_rejected(tmp_path: Path) -> None:
    service = _service(tmp_path)
    with pytest.raises(InvalidRequestError):
        service.get_day("repository", "2025-01-01", "python' OR 1=1 --")
