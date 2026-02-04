from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from .utils import ValidationError, normalize_date


@dataclass(frozen=True)
class ArchiveFile:
    kind: str
    path: Path
    date: date
    language: str | None
    items: list[str]


def _parse_archive_json(path: Path) -> tuple[date, str | None, list[str]]:
    payload = json.loads(path.read_text())
    if "date" not in payload or "list" not in payload:
        raise ValidationError(f"Archive JSON missing required fields: {path}")
    parsed_date = normalize_date(payload["date"])
    language = payload.get("language")
    if language == "":
        language = None
    items = payload.get("list")
    if not isinstance(items, list):
        raise ValidationError(f"Archive JSON list must be an array: {path}")
    return parsed_date, language, [str(item) for item in items]


def iter_archive_files(
    archive_root: Path,
    kind: str,
    *,
    year: int | None = None,
) -> Iterable[ArchiveFile]:
    kind_root = archive_root / kind
    if not kind_root.exists():
        raise ValidationError(f"Archive kind not found: {kind_root}")

    year_dirs = [kind_root / str(year)] if year else sorted(kind_root.glob("[0-9][0-9][0-9][0-9]"))
    for year_dir in year_dirs:
        if not year_dir.exists():
            continue
        for date_dir in sorted(year_dir.iterdir()):
            if not date_dir.is_dir():
                continue
            for json_path in sorted(date_dir.glob("*.json")):
                parsed_date, language, items = _parse_archive_json(json_path)
                yield ArchiveFile(
                    kind=kind,
                    path=json_path,
                    date=parsed_date,
                    language=language,
                    items=items,
                )
