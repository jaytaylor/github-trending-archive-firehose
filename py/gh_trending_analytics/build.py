from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .archive_reader import ArchiveFile, iter_archive_files
from .manifest import Manifest
from .utils import ValidationError, ensure_dir, iso_date, sort_languages

KIND_TABLES = {
    "repository": "repo_trend_entry",
    "developer": "dev_trend_entry",
}


@dataclass
class BuildResult:
    kind: str
    years_built: list[int]
    parquet_paths: list[Path]
    manifest_path: Path


def _repo_schema() -> pa.Schema:
    return pa.schema(
        [
            ("date", pa.date32()),
            ("language", pa.string()),
            ("rank", pa.int32()),
            ("full_name", pa.string()),
            ("owner", pa.string()),
            ("repo", pa.string()),
        ]
    )


def _dev_schema() -> pa.Schema:
    return pa.schema(
        [
            ("date", pa.date32()),
            ("language", pa.string()),
            ("rank", pa.int32()),
            ("username", pa.string()),
        ]
    )


def _schema_for_kind(kind: str) -> pa.Schema:
    if kind == "repository":
        return _repo_schema()
    if kind == "developer":
        return _dev_schema()
    raise ValidationError(f"Unsupported kind: {kind}")


def _table_name(kind: str) -> str:
    if kind not in KIND_TABLES:
        raise ValidationError(f"Unsupported kind: {kind}")
    return KIND_TABLES[kind]


def _parse_repo_row(entry: ArchiveFile, rank: int, full_name: str) -> dict:
    owner = None
    repo = None
    if "/" in full_name:
        owner, repo = full_name.split("/", 1)
    return {
        "date": entry.date,
        "language": entry.language,
        "rank": rank,
        "full_name": full_name,
        "owner": owner,
        "repo": repo,
    }


def _parse_dev_row(entry: ArchiveFile, rank: int, username: str) -> dict:
    return {
        "date": entry.date,
        "language": entry.language,
        "rank": rank,
        "username": username,
    }


def _collect_rows(entries: Iterable[ArchiveFile], kind: str) -> list[dict]:
    rows: list[dict] = []
    for entry in entries:
        for index, item in enumerate(entry.items):
            rank = index + 1
            if kind == "repository":
                rows.append(_parse_repo_row(entry, rank, item))
            else:
                rows.append(_parse_dev_row(entry, rank, item))
    rows.sort(key=lambda row: (row["date"], row["language"] or "", row["rank"]))
    return rows


def _read_existing_dates(path: Path) -> set[date]:
    if not path.exists():
        return set()
    table = pq.read_table(path, columns=["date"])
    return set(table["date"].to_pylist())


def _append_rows(existing_path: Path, new_rows: list[dict], schema: pa.Schema) -> pa.Table:
    if existing_path.exists():
        existing_table = pq.read_table(existing_path)
        new_table = pa.Table.from_pylist(new_rows, schema=schema)
        combined = pa.concat_tables([existing_table, new_table])
        combined = combined.sort_by(
            [
                ("date", "ascending"),
                ("language", "ascending"),
                ("rank", "ascending"),
            ]
        )
        return combined
    return pa.Table.from_pylist(new_rows, schema=schema)


def _write_parquet(table: pa.Table, path: Path) -> None:
    ensure_dir(path.parent)
    pq.write_table(table, path)


def _build_year(
    *,
    archive_root: Path,
    analytics_root: Path,
    kind: str,
    year: int,
    rebuild_year: bool,
) -> Path | None:
    schema = _schema_for_kind(kind)
    table_name = _table_name(kind)
    parquet_path = analytics_root / "parquet" / kind / f"year={year}" / f"{table_name}.parquet"

    entries = list(iter_archive_files(archive_root, kind, year=year))
    if not entries:
        return None

    if rebuild_year:
        rows = _collect_rows(entries, kind)
        table = pa.Table.from_pylist(rows, schema=schema)
        _write_parquet(table, parquet_path)
        return parquet_path

    existing_dates = _read_existing_dates(parquet_path)
    new_entries = [entry for entry in entries if entry.date not in existing_dates]
    if not new_entries and parquet_path.exists():
        return parquet_path

    rows = _collect_rows(new_entries, kind)
    table = _append_rows(parquet_path, rows, schema)
    _write_parquet(table, parquet_path)
    return parquet_path


def _manifest_from_archive(
    archive_root: Path, kind: str
) -> tuple[list[str], list[str | None], dict[str, list[str | None]], dict[str, int]]:
    dates: set[str] = set()
    languages: set[str | None] = set()
    languages_by_date: dict[str, set[str | None]] = {}
    row_counts_by_year: dict[str, int] = {}

    for entry in iter_archive_files(archive_root, kind):
        date_str = iso_date(entry.date)
        dates.add(date_str)
        languages.add(entry.language)
        languages_by_date.setdefault(date_str, set()).add(entry.language)
        year_str = str(entry.date.year)
        row_counts_by_year[year_str] = row_counts_by_year.get(year_str, 0) + len(entry.items)

    languages_by_date_sorted = {
        key: sort_languages(value) for key, value in languages_by_date.items()
    }
    return sorted(dates), sort_languages(languages), languages_by_date_sorted, row_counts_by_year


def build_kind(
    *,
    archive_root: Path,
    analytics_root: Path,
    kind: str,
    year: int | None = None,
    rebuild_year: bool = False,
) -> BuildResult:
    if kind not in KIND_TABLES:
        raise ValidationError(f"Unsupported kind: {kind}")

    archive_root = archive_root.resolve()
    analytics_root = analytics_root.resolve()
    if not archive_root.exists():
        raise ValidationError(f"Archive root does not exist: {archive_root}")

    years: list[int]
    if year is not None:
        years = [year]
    else:
        kind_root = archive_root / kind
        if not kind_root.exists():
            raise ValidationError(f"Archive kind not found: {kind_root}")
        years = sorted([int(path.name) for path in kind_root.glob("[0-9][0-9][0-9][0-9]")])

    parquet_paths: list[Path] = []
    for target_year in years:
        path = _build_year(
            archive_root=archive_root,
            analytics_root=analytics_root,
            kind=kind,
            year=target_year,
            rebuild_year=rebuild_year,
        )
        if path is not None:
            parquet_paths.append(path)

    manifest = Manifest.load(analytics_root / "parquet" / "manifest.json")
    dates, languages, languages_by_date, row_counts_by_year = _manifest_from_archive(
        archive_root, kind
    )
    manifest.update_kind(
        kind,
        dates=dates,
        languages=languages,
        languages_by_date=languages_by_date,
        row_counts_by_year=row_counts_by_year,
    )
    manifest_path = analytics_root / "parquet" / "manifest.json"
    manifest.save(manifest_path)

    return BuildResult(
        kind=kind,
        years_built=years,
        parquet_paths=parquet_paths,
        manifest_path=manifest_path,
    )
