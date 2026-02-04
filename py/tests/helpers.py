from __future__ import annotations

from pathlib import Path

from gh_trending_analytics.build import build_kind
from gh_trending_analytics.manifest import Manifest

FIXTURE_ARCHIVE = Path(__file__).parent / "fixtures" / "archive"


def build_fixture(tmp_path: Path, *, kinds: list[str] | None = None) -> Path:
    analytics_root = tmp_path / "analytics"
    kinds = kinds or ["repository", "developer"]
    for kind in kinds:
        build_kind(
            archive_root=FIXTURE_ARCHIVE,
            analytics_root=analytics_root,
            kind=kind,
            rebuild_year=True,
        )
    return analytics_root


def load_manifest(analytics_root: Path) -> Manifest:
    return Manifest.load(analytics_root / "parquet" / "manifest.json")
