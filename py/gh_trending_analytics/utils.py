from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path


class ValidationError(ValueError):
    pass


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError(f"Invalid date format: {value}") from exc


def normalize_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return parse_date(value)


def iso_date(value: date) -> str:
    return value.isoformat()


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    lowered = value.strip().lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n"}:
        return False
    raise ValidationError(f"Invalid boolean value: {value}")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sort_languages(languages: Iterable[str | None]) -> list[str | None]:
    def key(value: str | None) -> tuple[int, str]:
        if value is None:
            return (1, "")
        return (0, value)

    return sorted(set(languages), key=key)


@dataclass(frozen=True)
class CacheKey:
    prefix: str
    payload: dict

    def as_str(self) -> str:
        raw = json.dumps(self.payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return f"{self.prefix}:{raw}"
