from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
from gh_trending_analytics.build import KIND_TABLES
from helpers import build_fixture, load_manifest


def test_build_manifest_and_parquet(tmp_path: Path) -> None:
    analytics_root = build_fixture(tmp_path)
    manifest = load_manifest(analytics_root)

    repo_manifest = manifest.kinds["repository"]
    dev_manifest = manifest.kinds["developer"]

    assert repo_manifest.min_date == "2025-01-01"
    assert repo_manifest.max_date == "2025-01-02"
    assert dev_manifest.min_date == "2025-01-01"
    assert dev_manifest.max_date == "2025-01-02"

    assert "python" in repo_manifest.languages
    assert None in repo_manifest.languages
    assert "c++" in repo_manifest.languages
    assert "c#" in repo_manifest.languages

    assert repo_manifest.row_counts_by_year["2025"] == 11
    assert dev_manifest.row_counts_by_year["2025"] == 9

    repo_table = KIND_TABLES["repository"]
    dev_table = KIND_TABLES["developer"]
    repo_path = analytics_root / "parquet" / "repository" / "year=2025" / f"{repo_table}.parquet"
    dev_path = analytics_root / "parquet" / "developer" / "year=2025" / f"{dev_table}.parquet"

    assert repo_path.exists()
    assert dev_path.exists()

    assert pq.read_metadata(repo_path).num_rows == 11
    assert pq.read_metadata(dev_path).num_rows == 9
