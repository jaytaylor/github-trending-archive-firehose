from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from .errors import InvalidRequestError, NotFoundError
from .manifest import Manifest
from .utils import ValidationError, parse_date

VALID_KINDS = {"repository", "developer"}
VALID_PRESENCE = {"day", "occurrence"}


@dataclass
class QueryConfig:
    analytics_root: Path
    manifest: Manifest | None = None
    use_rollups: bool = True

    def load_manifest(self) -> Manifest:
        if self.manifest is not None:
            return self.manifest
        return Manifest.load(self.analytics_root / "parquet" / "manifest.json")


class DuckDBQueryService:
    def __init__(self, config: QueryConfig) -> None:
        self._config = config
        self._manifest = config.load_manifest()

    @property
    def manifest(self) -> Manifest:
        return self._manifest

    def _validate_kind(self, kind: str) -> None:
        if kind not in VALID_KINDS:
            raise InvalidRequestError(f"Unsupported kind: {kind}")

    def _validate_presence(self, presence: str) -> None:
        if presence not in VALID_PRESENCE:
            raise InvalidRequestError(f"Unsupported presence: {presence}")

    def _manifest_kind(self, kind: str):
        self._validate_kind(kind)
        if kind not in self._manifest.kinds:
            raise NotFoundError(f"No manifest data for kind: {kind}")
        return self._manifest.kinds[kind]

    def _validate_date_exists(self, kind: str, day: date) -> None:
        manifest_kind = self._manifest_kind(kind)
        if day.isoformat() not in manifest_kind.dates:
            raise NotFoundError(f"Date {day.isoformat()} not found for kind={kind}")

    def _validate_language(
        self, kind: str, language: str | None, *, day: date | None = None
    ) -> None:
        if language is None or language == "__all__":
            return
        manifest_kind = self._manifest_kind(kind)
        if day is not None and manifest_kind.languages_by_date:
            day_languages = manifest_kind.languages_by_date.get(day.isoformat(), [])
            if language not in day_languages:
                raise InvalidRequestError(f"Unsupported language for {day.isoformat()}: {language}")
            return
        if language not in manifest_kind.languages:
            raise InvalidRequestError(f"Unsupported language: {language}")

    def _parse_date(self, value: str) -> date:
        try:
            return parse_date(value)
        except ValidationError as exc:
            raise InvalidRequestError(str(exc)) from exc

    def _normalize_language_param(self, language: str | None) -> str | None:
        if language is None or language == "__all__":
            return None
        return language

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect()

    def _parquet_glob(self, kind: str) -> str:
        table = "repo_trend_entry" if kind == "repository" else "dev_trend_entry"
        return str(self._config.analytics_root / "parquet" / kind / "year=*" / f"{table}.parquet")

    def _rollup_glob(self, kind: str) -> str:
        table = "repo_day_presence" if kind == "repository" else "dev_day_presence"
        return str(self._config.analytics_root / "rollups" / kind / "year=*" / f"{table}.parquet")

    def list_dates(self, kind: str) -> list[str]:
        manifest_kind = self._manifest_kind(kind)
        return list(manifest_kind.dates)

    def list_languages(self, kind: str) -> list[str | None]:
        manifest_kind = self._manifest_kind(kind)
        return list(manifest_kind.languages)

    def get_day(self, kind: str, day: str, language: str | None) -> list[dict[str, Any]]:
        self._validate_kind(kind)
        parsed = self._parse_date(day)
        self._validate_date_exists(kind, parsed)
        language_value = "__all__" if language is None else language
        self._validate_language(kind, language_value, day=parsed)

        parquet_glob = self._parquet_glob(kind)
        con = self._connect()
        if kind == "repository":
            sql = (
                "SELECT full_name, owner, repo, rank "
                "FROM read_parquet(?) "
                "WHERE date = ? AND (language = ? OR (language IS NULL AND ? = '__all__')) "
                "ORDER BY rank ASC"
            )
            rows = con.execute(
                sql, [parquet_glob, parsed, language_value, language_value]
            ).fetchall()
            return [
                {
                    "rank": row[3],
                    "full_name": row[0],
                    "owner": row[1],
                    "repo": row[2],
                }
                for row in rows
            ]
        sql = (
            "SELECT username, rank "
            "FROM read_parquet(?) "
            "WHERE date = ? AND (language = ? OR (language IS NULL AND ? = '__all__')) "
            "ORDER BY rank ASC"
        )
        rows = con.execute(sql, [parquet_glob, parsed, language_value, language_value]).fetchall()
        return [{"rank": row[1], "username": row[0]} for row in rows]

    def top_reappearing(
        self,
        kind: str,
        start: str,
        end: str,
        *,
        language: str | None,
        presence: str,
        include_all_languages: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        self._validate_kind(kind)
        self._validate_presence(presence)
        language = self._normalize_language_param(language)
        start_date = self._parse_date(start)
        end_date = self._parse_date(end)
        if start_date > end_date:
            raise InvalidRequestError("Start date must be <= end date")
        self._validate_language(kind, language)

        parquet_glob = self._parquet_glob(kind)
        con = self._connect()

        if (
            self._config.use_rollups
            and presence == "day"
            and language is None
            and (self._config.analytics_root / "rollups").exists()
        ):
            try:
                return self._top_reappearing_rollup(
                    con,
                    kind,
                    start_date,
                    end_date,
                    include_all_languages,
                    limit,
                )
            except Exception:
                # Fall back to raw parquet on any rollup failure.
                pass

        if kind == "repository":
            count_expr = "COUNT(DISTINCT date)" if presence == "day" else "COUNT(*)"
            sql = (
                f"SELECT full_name, owner, {count_expr} AS days_present, MIN(rank) AS best_rank "
                "FROM read_parquet(?) "
                "WHERE date BETWEEN ? AND ? "
                "AND (? IS NULL OR language = ?) "
                "AND (? OR language IS NOT NULL) "
                "GROUP BY full_name, owner "
                "ORDER BY days_present DESC, best_rank ASC, full_name ASC "
                "LIMIT ?"
            )
            rows = con.execute(
                sql,
                [
                    parquet_glob,
                    start_date,
                    end_date,
                    language,
                    language,
                    include_all_languages,
                    limit,
                ],
            ).fetchall()
            return [
                {
                    "full_name": row[0],
                    "owner": row[1],
                    "days_present": row[2],
                    "best_rank": row[3],
                }
                for row in rows
            ]

        count_expr = "COUNT(DISTINCT date)" if presence == "day" else "COUNT(*)"
        sql = (
            f"SELECT username, {count_expr} AS days_present, MIN(rank) AS best_rank "
            "FROM read_parquet(?) "
            "WHERE date BETWEEN ? AND ? "
            "AND (? IS NULL OR language = ?) "
            "AND (? OR language IS NOT NULL) "
            "GROUP BY username "
            "ORDER BY days_present DESC, best_rank ASC, username ASC "
            "LIMIT ?"
        )
        rows = con.execute(
            sql,
            [
                parquet_glob,
                start_date,
                end_date,
                language,
                language,
                include_all_languages,
                limit,
            ],
        ).fetchall()
        return [
            {
                "username": row[0],
                "days_present": row[1],
                "best_rank": row[2],
            }
            for row in rows
        ]

    def _top_reappearing_rollup(
        self,
        con: duckdb.DuckDBPyConnection,
        kind: str,
        start_date: date,
        end_date: date,
        include_all_languages: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        rollup_glob = self._rollup_glob(kind)
        if kind == "repository":
            sql = (
                "SELECT full_name, owner, COUNT(*) AS days_present, "
                "MIN(CASE WHEN ? THEN best_rank_any ELSE best_rank_non_null END) AS best_rank "
                "FROM read_parquet(?) "
                "WHERE date BETWEEN ? AND ? "
                "AND (? OR non_null_languages > 0) "
                "GROUP BY full_name, owner "
                "ORDER BY days_present DESC, best_rank ASC, full_name ASC "
                "LIMIT ?"
            )
            rows = con.execute(
                sql,
                [
                    include_all_languages,
                    rollup_glob,
                    start_date,
                    end_date,
                    include_all_languages,
                    limit,
                ],
            ).fetchall()
            return [
                {
                    "full_name": row[0],
                    "owner": row[1],
                    "days_present": row[2],
                    "best_rank": row[3],
                }
                for row in rows
            ]

        sql = (
            "SELECT username, COUNT(*) AS days_present, "
            "MIN(CASE WHEN ? THEN best_rank_any ELSE best_rank_non_null END) AS best_rank "
            "FROM read_parquet(?) "
            "WHERE date BETWEEN ? AND ? "
            "AND (? OR non_null_languages > 0) "
            "GROUP BY username "
            "ORDER BY days_present DESC, best_rank ASC, username ASC "
            "LIMIT ?"
        )
        rows = con.execute(
            sql,
            [
                include_all_languages,
                rollup_glob,
                start_date,
                end_date,
                include_all_languages,
                limit,
            ],
        ).fetchall()
        return [
            {
                "username": row[0],
                "days_present": row[1],
                "best_rank": row[2],
            }
            for row in rows
        ]

    def top_owners(
        self,
        start: str,
        end: str,
        *,
        language: str | None,
        include_all_languages: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        language = self._normalize_language_param(language)
        start_date = self._parse_date(start)
        end_date = self._parse_date(end)
        if start_date > end_date:
            raise InvalidRequestError("Start date must be <= end date")
        self._validate_language("repository", language)

        parquet_glob = self._parquet_glob("repository")
        con = self._connect()
        sql = (
            "SELECT owner, COUNT(DISTINCT full_name) AS repos_present, MIN(rank) AS best_rank "
            "FROM read_parquet(?) "
            "WHERE date BETWEEN ? AND ? "
            "AND (? IS NULL OR language = ?) "
            "AND (? OR language IS NOT NULL) "
            "GROUP BY owner "
            "ORDER BY repos_present DESC, best_rank ASC, owner ASC "
            "LIMIT ?"
        )
        rows = con.execute(
            sql,
            [parquet_glob, start_date, end_date, language, language, include_all_languages, limit],
        ).fetchall()
        return [{"owner": row[0], "repos_present": row[1], "best_rank": row[2]} for row in rows]

    def top_languages(
        self,
        start: str,
        end: str,
        *,
        kind: str | None,
        include_all_languages: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        start_date = self._parse_date(start)
        end_date = self._parse_date(end)
        if start_date > end_date:
            raise InvalidRequestError("Start date must be <= end date")

        con = self._connect()
        if kind:
            self._validate_kind(kind)
            parquet_glob = self._parquet_glob(kind)
            sql = (
                "SELECT language, COUNT(*) AS entries "
                "FROM read_parquet(?) "
                "WHERE date BETWEEN ? AND ? "
                "AND (? OR language IS NOT NULL) "
                "GROUP BY language "
                "ORDER BY entries DESC, language ASC "
                "LIMIT ?"
            )
            rows = con.execute(
                sql, [parquet_glob, start_date, end_date, include_all_languages, limit]
            ).fetchall()
        else:
            repo_glob = self._parquet_glob("repository")
            dev_glob = self._parquet_glob("developer")
            sql = (
                "SELECT language, COUNT(*) AS entries FROM ("
                "  SELECT language FROM read_parquet(?) WHERE date BETWEEN ? AND ? "
                "  UNION ALL "
                "  SELECT language FROM read_parquet(?) WHERE date BETWEEN ? AND ? "
                ") "
                "WHERE (? OR language IS NOT NULL) "
                "GROUP BY language "
                "ORDER BY entries DESC, language ASC "
                "LIMIT ?"
            )
            rows = con.execute(
                sql,
                [
                    repo_glob,
                    start_date,
                    end_date,
                    dev_glob,
                    start_date,
                    end_date,
                    include_all_languages,
                    limit,
                ],
            ).fetchall()

        return [{"language": row[0], "entries": row[1]} for row in rows]

    def top_newcomers(
        self,
        kind: str,
        start: str,
        end: str,
        *,
        language: str | None,
        include_all_languages: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        self._validate_kind(kind)
        language = self._normalize_language_param(language)
        start_date = self._parse_date(start)
        end_date = self._parse_date(end)
        if start_date > end_date:
            raise InvalidRequestError("Start date must be <= end date")
        self._validate_language(kind, language)

        parquet_glob = self._parquet_glob(kind)
        con = self._connect()
        if kind == "repository":
            sql = (
                "WITH first_seen AS ("
                "  SELECT full_name, owner, MIN(date) AS first_seen, MIN(rank) AS best_rank "
                "  FROM read_parquet(?) "
                "  WHERE (? IS NULL OR language = ?) "
                "    AND (? OR language IS NOT NULL) "
                "  GROUP BY full_name, owner"
                ") "
                "SELECT full_name, owner, first_seen, best_rank "
                "FROM first_seen "
                "WHERE first_seen BETWEEN ? AND ? "
                "ORDER BY first_seen DESC, best_rank ASC, full_name ASC "
                "LIMIT ?"
            )
            rows = con.execute(
                sql,
                [
                    parquet_glob,
                    language,
                    language,
                    include_all_languages,
                    start_date,
                    end_date,
                    limit,
                ],
            ).fetchall()
            return [
                {
                    "full_name": row[0],
                    "owner": row[1],
                    "first_seen": row[2].isoformat() if row[2] else None,
                    "best_rank": row[3],
                }
                for row in rows
            ]

        sql = (
            "WITH first_seen AS ("
            "  SELECT username, MIN(date) AS first_seen, MIN(rank) AS best_rank "
            "  FROM read_parquet(?) "
            "  WHERE (? IS NULL OR language = ?) "
            "    AND (? OR language IS NOT NULL) "
            "  GROUP BY username"
            ") "
            "SELECT username, first_seen, best_rank "
            "FROM first_seen "
            "WHERE first_seen BETWEEN ? AND ? "
            "ORDER BY first_seen DESC, best_rank ASC, username ASC "
            "LIMIT ?"
        )
        rows = con.execute(
            sql,
            [
                parquet_glob,
                language,
                language,
                include_all_languages,
                start_date,
                end_date,
                limit,
            ],
        ).fetchall()
        return [
            {
                "username": row[0],
                "first_seen": row[1].isoformat() if row[1] else None,
                "best_rank": row[2],
            }
            for row in rows
        ]

    def top_streaks(
        self,
        kind: str,
        start: str,
        end: str,
        *,
        language: str | None,
        include_all_languages: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        self._validate_kind(kind)
        language = self._normalize_language_param(language)
        start_date = self._parse_date(start)
        end_date = self._parse_date(end)
        if start_date > end_date:
            raise InvalidRequestError("Start date must be <= end date")
        self._validate_language(kind, language)

        con = self._connect()
        if (
            self._config.use_rollups
            and language is None
            and (self._config.analytics_root / "rollups").exists()
        ):
            try:
                return self._top_streaks_rollup(
                    con,
                    kind,
                    start_date,
                    end_date,
                    include_all_languages,
                    limit,
                )
            except Exception:
                pass

        parquet_glob = self._parquet_glob(kind)
        if kind == "repository":
            sql = (
                "WITH base AS ("
                "  SELECT date, full_name, owner, MIN(rank) AS best_rank "
                "  FROM read_parquet(?) "
                "  WHERE date BETWEEN ? AND ? "
                "    AND (? IS NULL OR language = ?) "
                "    AND (? OR language IS NOT NULL)"
                "  GROUP BY date, full_name, owner"
                "), ordered AS ("
                "  SELECT *, "
                "    DATEDIFF('day', LAG(date) OVER (PARTITION BY full_name ORDER BY date), date) AS gap "
                "  FROM base"
                "), groups AS ("
                "  SELECT *, "
                "    SUM(CASE WHEN gap IS NULL OR gap != 1 THEN 1 ELSE 0 END) "
                "      OVER (PARTITION BY full_name ORDER BY date) AS grp "
                "  FROM ordered"
                "), streaks AS ("
                "  SELECT full_name, owner, MIN(date) AS streak_start, MAX(date) AS streak_end, "
                "    COUNT(*) AS streak_len, MIN(best_rank) AS best_rank "
                "  FROM groups "
                "  GROUP BY full_name, owner, grp"
                "), longest AS ("
                "  SELECT *, "
                "    ROW_NUMBER() OVER (PARTITION BY full_name ORDER BY streak_len DESC, streak_end DESC) AS rn "
                "  FROM streaks"
                ") "
                "SELECT full_name, owner, streak_start, streak_end, streak_len, best_rank "
                "FROM longest WHERE rn = 1 "
                "ORDER BY streak_len DESC, best_rank ASC, full_name ASC "
                "LIMIT ?"
            )
            rows = con.execute(
                sql,
                [
                    parquet_glob,
                    start_date,
                    end_date,
                    language,
                    language,
                    include_all_languages,
                    limit,
                ],
            ).fetchall()
            return [
                {
                    "full_name": row[0],
                    "owner": row[1],
                    "streak_start": row[2].isoformat(),
                    "streak_end": row[3].isoformat(),
                    "streak_len": row[4],
                    "best_rank": row[5],
                }
                for row in rows
            ]

        sql = (
            "WITH base AS ("
            "  SELECT date, username, MIN(rank) AS best_rank "
            "  FROM read_parquet(?) "
            "  WHERE date BETWEEN ? AND ? "
            "    AND (? IS NULL OR language = ?) "
            "    AND (? OR language IS NOT NULL)"
            "  GROUP BY date, username"
            "), ordered AS ("
            "  SELECT *, "
            "    DATEDIFF('day', LAG(date) OVER (PARTITION BY username ORDER BY date), date) AS gap "
            "  FROM base"
            "), groups AS ("
            "  SELECT *, "
            "    SUM(CASE WHEN gap IS NULL OR gap != 1 THEN 1 ELSE 0 END) "
            "      OVER (PARTITION BY username ORDER BY date) AS grp "
            "  FROM ordered"
            "), streaks AS ("
            "  SELECT username, MIN(date) AS streak_start, MAX(date) AS streak_end, "
            "    COUNT(*) AS streak_len, MIN(best_rank) AS best_rank "
            "  FROM groups "
            "  GROUP BY username, grp"
            "), longest AS ("
            "  SELECT *, "
            "    ROW_NUMBER() OVER (PARTITION BY username ORDER BY streak_len DESC, streak_end DESC) AS rn "
            "  FROM streaks"
            ") "
            "SELECT username, streak_start, streak_end, streak_len, best_rank "
            "FROM longest WHERE rn = 1 "
            "ORDER BY streak_len DESC, best_rank ASC, username ASC "
            "LIMIT ?"
        )
        rows = con.execute(
            sql,
            [
                parquet_glob,
                start_date,
                end_date,
                language,
                language,
                include_all_languages,
                limit,
            ],
        ).fetchall()
        return [
            {
                "username": row[0],
                "streak_start": row[1].isoformat(),
                "streak_end": row[2].isoformat(),
                "streak_len": row[3],
                "best_rank": row[4],
            }
            for row in rows
        ]

    def _top_streaks_rollup(
        self,
        con: duckdb.DuckDBPyConnection,
        kind: str,
        start_date: date,
        end_date: date,
        include_all_languages: bool,
        limit: int,
    ) -> list[dict[str, Any]]:
        rollup_glob = self._rollup_glob(kind)
        if kind == "repository":
            sql = (
                "WITH base AS ("
                "  SELECT date, full_name, owner, "
                "    CASE WHEN ? THEN best_rank_any ELSE best_rank_non_null END AS best_rank "
                "  FROM read_parquet(?) "
                "  WHERE date BETWEEN ? AND ? "
                "    AND (? OR non_null_languages > 0)"
                "), ordered AS ("
                "  SELECT *, "
                "    DATEDIFF('day', LAG(date) OVER (PARTITION BY full_name ORDER BY date), date) AS gap "
                "  FROM base"
                "), groups AS ("
                "  SELECT *, "
                "    SUM(CASE WHEN gap IS NULL OR gap != 1 THEN 1 ELSE 0 END) "
                "      OVER (PARTITION BY full_name ORDER BY date) AS grp "
                "  FROM ordered"
                "), streaks AS ("
                "  SELECT full_name, owner, MIN(date) AS streak_start, MAX(date) AS streak_end, "
                "    COUNT(*) AS streak_len, MIN(best_rank) AS best_rank "
                "  FROM groups "
                "  GROUP BY full_name, owner, grp"
                "), longest AS ("
                "  SELECT *, "
                "    ROW_NUMBER() OVER (PARTITION BY full_name ORDER BY streak_len DESC, streak_end DESC) AS rn "
                "  FROM streaks"
                ") "
                "SELECT full_name, owner, streak_start, streak_end, streak_len, best_rank "
                "FROM longest WHERE rn = 1 "
                "ORDER BY streak_len DESC, best_rank ASC, full_name ASC "
                "LIMIT ?"
            )
            rows = con.execute(
                sql,
                [
                    include_all_languages,
                    rollup_glob,
                    start_date,
                    end_date,
                    include_all_languages,
                    limit,
                ],
            ).fetchall()
            return [
                {
                    "full_name": row[0],
                    "owner": row[1],
                    "streak_start": row[2].isoformat(),
                    "streak_end": row[3].isoformat(),
                    "streak_len": row[4],
                    "best_rank": row[5],
                }
                for row in rows
            ]

        sql = (
            "WITH base AS ("
            "  SELECT date, username, "
            "    CASE WHEN ? THEN best_rank_any ELSE best_rank_non_null END AS best_rank "
            "  FROM read_parquet(?) "
            "  WHERE date BETWEEN ? AND ? "
            "    AND (? OR non_null_languages > 0)"
            "), ordered AS ("
            "  SELECT *, "
            "    DATEDIFF('day', LAG(date) OVER (PARTITION BY username ORDER BY date), date) AS gap "
            "  FROM base"
            "), groups AS ("
            "  SELECT *, "
            "    SUM(CASE WHEN gap IS NULL OR gap != 1 THEN 1 ELSE 0 END) "
            "      OVER (PARTITION BY username ORDER BY date) AS grp "
            "  FROM ordered"
            "), streaks AS ("
            "  SELECT username, MIN(date) AS streak_start, MAX(date) AS streak_end, "
            "    COUNT(*) AS streak_len, MIN(best_rank) AS best_rank "
            "  FROM groups "
            "  GROUP BY username, grp"
            "), longest AS ("
            "  SELECT *, "
            "    ROW_NUMBER() OVER (PARTITION BY username ORDER BY streak_len DESC, streak_end DESC) AS rn "
            "  FROM streaks"
            ") "
            "SELECT username, streak_start, streak_end, streak_len, best_rank "
            "FROM longest WHERE rn = 1 "
            "ORDER BY streak_len DESC, best_rank ASC, username ASC "
            "LIMIT ?"
        )
        rows = con.execute(
            sql,
            [
                include_all_languages,
                rollup_glob,
                start_date,
                end_date,
                include_all_languages,
                limit,
            ],
        ).fetchall()
        return [
            {
                "username": row[0],
                "streak_start": row[1].isoformat(),
                "streak_end": row[2].isoformat(),
                "streak_len": row[3],
                "best_rank": row[4],
            }
            for row in rows
        ]
