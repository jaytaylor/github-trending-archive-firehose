from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from gh_trending_analytics.query import DuckDBQueryService, QueryConfig
from gh_trending_analytics.rollup import rollup_kind
from gh_trending_analytics.utils import ValidationError
from helpers import build_fixture


def test_rollup_builder_outputs_files(tmp_path: Path) -> None:
    analytics_root = build_fixture(tmp_path)
    rollup_kind(analytics_root=analytics_root, kind="repository", from_date=None)
    output_path = (
        analytics_root / "rollups" / "repository" / "year=2025" / "repo_day_presence.parquet"
    )
    assert output_path.exists()


def test_rollup_invalid_kind(tmp_path: Path) -> None:
    analytics_root = build_fixture(tmp_path)
    with pytest.raises(ValidationError):
        rollup_kind(analytics_root=analytics_root, kind="invalid", from_date=None)


def test_rollup_bad_input(tmp_path: Path) -> None:
    analytics_root = tmp_path / "analytics"
    bad_path = analytics_root / "parquet" / "repository" / "year=2025" / "repo_trend_entry.parquet"
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pydict({"date": pa.array([date(2025, 1, 1)], type=pa.date32())})
    pq.write_table(table, bad_path)

    with pytest.raises(ValidationError, match="Rollup query failed"):
        rollup_kind(analytics_root=analytics_root, kind="repository", from_date=None)


def test_rollup_corrupt_fallback(tmp_path: Path) -> None:
    analytics_root = build_fixture(tmp_path)
    rollup_kind(analytics_root=analytics_root, kind="repository", from_date=None)
    rollup_path = (
        analytics_root / "rollups" / "repository" / "year=2025" / "repo_day_presence.parquet"
    )
    rollup_path.write_text("corrupt")

    service = DuckDBQueryService(QueryConfig(analytics_root=analytics_root, use_rollups=True))
    results = service.top_reappearing(
        "repository",
        "2025-01-01",
        "2025-01-02",
        language=None,
        presence="day",
        include_all_languages=False,
        limit=5,
    )
    assert results
