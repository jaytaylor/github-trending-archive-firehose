from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .utils import sort_languages, utc_now_iso


@dataclass
class ManifestKind:
    min_date: str | None
    max_date: str | None
    dates: list[str]
    languages: list[str | None]
    languages_by_date: dict[str, list[str | None]]
    row_counts_by_year: dict[str, int]

    @classmethod
    def empty(cls) -> ManifestKind:
        return cls(
            min_date=None,
            max_date=None,
            dates=[],
            languages=[],
            languages_by_date={},
            row_counts_by_year={},
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> ManifestKind:
        return cls(
            min_date=payload.get("min_date"),
            max_date=payload.get("max_date"),
            dates=list(payload.get("dates", [])),
            languages=list(payload.get("languages", [])),
            languages_by_date={
                key: list(value) for key, value in payload.get("languages_by_date", {}).items()
            },
            row_counts_by_year={
                str(key): int(value) for key, value in payload.get("row_counts_by_year", {}).items()
            },
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "min_date": self.min_date,
            "max_date": self.max_date,
            "dates": list(self.dates),
            "languages": list(self.languages),
            "languages_by_date": {
                key: list(value) for key, value in self.languages_by_date.items()
            },
            "row_counts_by_year": dict(self.row_counts_by_year),
        }


@dataclass
class Manifest:
    generated_at: str
    kinds: dict[str, ManifestKind]

    @classmethod
    def empty(cls) -> Manifest:
        return cls(generated_at=utc_now_iso(), kinds={})

    @classmethod
    def load(cls, path: Path) -> Manifest:
        if not path.exists():
            return cls.empty()
        payload = json.loads(path.read_text())
        kinds_payload = payload.get("kinds", {})
        kinds = {key: ManifestKind.from_dict(value) for key, value in kinds_payload.items()}
        return cls(generated_at=payload.get("generated_at", utc_now_iso()), kinds=kinds)

    def ensure_kind(self, kind: str) -> ManifestKind:
        if kind not in self.kinds:
            self.kinds[kind] = ManifestKind.empty()
        return self.kinds[kind]

    def update_kind(
        self,
        kind: str,
        *,
        dates: list[str],
        languages: list[str | None],
        languages_by_date: dict[str, list[str | None]],
        row_counts_by_year: dict[str, int],
    ) -> None:
        sorted_dates = sorted(dates)
        self.kinds[kind] = ManifestKind(
            min_date=sorted_dates[0] if sorted_dates else None,
            max_date=sorted_dates[-1] if sorted_dates else None,
            dates=sorted_dates,
            languages=sort_languages(languages),
            languages_by_date={
                key: sort_languages(value) for key, value in languages_by_date.items()
            },
            row_counts_by_year=dict(row_counts_by_year),
        )
        self.generated_at = utc_now_iso()

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "kinds": {key: value.to_dict() for key, value in self.kinds.items()},
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True))
