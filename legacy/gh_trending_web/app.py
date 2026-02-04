from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from gh_trending_analytics.cache import ResultCache
from gh_trending_analytics.errors import InvalidRequestError, NotFoundError
from gh_trending_analytics.manifest import Manifest
from gh_trending_analytics.query import DuckDBQueryService, QueryConfig
from gh_trending_analytics.utils import CacheKey, ValidationError, parse_bool


def _error_response(error: str, message: str, hint: str | None = None) -> dict[str, Any]:
    payload = {"error": error, "message": message}
    if hint:
        payload["hint"] = hint
    return payload


def _parse_limit(limit: str | int | None) -> int:
    if limit is None:
        return 50
    if isinstance(limit, int):
        value = limit
    else:
        try:
            value = int(limit)
        except (TypeError, ValueError) as exc:
            raise InvalidRequestError("limit must be between 1 and 500") from exc
    if value < 1 or value > 500:
        raise InvalidRequestError("limit must be between 1 and 500")
    return value


def _parse_presence(presence: str | None) -> str:
    if presence is None:
        return "day"
    if presence not in {"day", "occurrence"}:
        raise InvalidRequestError("presence must be 'day' or 'occurrence'")
    return presence


def _parse_include_all_languages(value: str | None, default: bool = False) -> bool:
    try:
        return parse_bool(value, default=default)
    except ValidationError as exc:
        raise InvalidRequestError(str(exc)) from exc


def _default_date(manifest: Manifest, kind: str) -> str:
    if kind not in manifest.kinds:
        raise NotFoundError(f"No manifest data for kind: {kind}")
    if not manifest.kinds[kind].dates:
        raise NotFoundError(f"No dates available for kind: {kind}")
    return manifest.kinds[kind].max_date or manifest.kinds[kind].dates[-1]


def _date_hint(manifest: Manifest, kind: str, *, limit: int = 5) -> str | None:
    if kind not in manifest.kinds:
        return None
    dates = manifest.kinds[kind].dates
    if not dates:
        return None
    sample = ", ".join(dates[-limit:])
    return f"Try one of: {sample}"


def create_app(*, analytics_root: Path) -> FastAPI:
    logger = logging.getLogger("gh_trending_web.cache")
    manifest = Manifest.load(analytics_root / "parquet" / "manifest.json")
    query_service = DuckDBQueryService(
        QueryConfig(analytics_root=analytics_root, manifest=manifest)
    )
    cache = ResultCache(max_size=2048, default_ttl=300.0)

    app = FastAPI()
    templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
    app.state.cache = cache
    app.state.manifest = manifest
    app.state.query_service = query_service

    def _cache_key(prefix: str, payload: dict[str, Any]) -> str:
        return CacheKey(prefix, payload).as_str()

    def _cached_day(kind: str, date: str, language: str) -> list[dict[str, Any]]:
        key = _cache_key(
            "day",
            {"kind": kind, "date": date, "language": language},
        )
        cached = cache.get(key)
        if cached is not None:
            return cached
        entries = query_service.get_day(kind, date, language)
        cache.set(key, entries)
        return entries

    def _cached_toplist(prefix: str, payload: dict[str, Any], loader) -> Any:
        key = _cache_key(prefix, payload)
        cached = cache.get(key)
        if cached is not None:
            return cached
        result = loader()
        cache.set(key, result)
        return result

    def _neighbor_dates(kind: str, current_date: str) -> tuple[str | None, str | None]:
        if kind not in manifest.kinds:
            return None, None
        dates = manifest.kinds[kind].dates
        if current_date not in dates:
            return None, None
        idx = dates.index(current_date)
        prev_date = dates[idx - 1] if idx > 0 else None
        next_date = dates[idx + 1] if idx < len(dates) - 1 else None
        return prev_date, next_date

    def _prewarm_day(kind: str, date: str, language: str) -> None:
        key = _cache_key("day", {"kind": kind, "date": date, "language": language})
        if cache.get(key) is not None:
            return
        try:
            entries = query_service.get_day(kind, date, language)
        except Exception:
            cache.stats.prewarm_failure += 1
            logger.info("prewarm_failure kind=%s date=%s language=%s", kind, date, language)
            return
        cache.set(key, entries)
        cache.stats.prewarm_success += 1
        logger.info("prewarm_success kind=%s date=%s language=%s", kind, date, language)

    @app.exception_handler(InvalidRequestError)
    async def _invalid_request_handler(_: Request, exc: InvalidRequestError) -> JSONResponse:
        return JSONResponse(status_code=400, content=_error_response("invalid_request", str(exc)))

    @app.exception_handler(NotFoundError)
    async def _not_found_handler(_: Request, exc: NotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content=_error_response("not_found", str(exc)))

    @app.get("/repositories", response_class=HTMLResponse)
    async def repositories(request: Request, date: str | None = None, language: str | None = None):
        kind = "repository"
        selected_date = date or _default_date(manifest, kind)
        manifest_kind = manifest.kinds.get(kind)
        dates = manifest_kind.dates if manifest_kind else []
        languages_by_date = manifest_kind.languages_by_date if manifest_kind else {}
        global_languages = manifest_kind.languages if manifest_kind else []
        available = languages_by_date.get(selected_date, global_languages)
        normalized = [("__all__" if value is None else value) for value in available]
        if language:
            selected_language = language
        elif "__all__" in normalized:
            selected_language = "__all__"
        else:
            selected_language = normalized[0] if normalized else "__all__"

        return templates.TemplateResponse(
            "day.html",
            {
                "request": request,
                "kind": kind,
                "title": "Repositories",
                "dates": dates,
                "languages_by_date": languages_by_date,
                "global_languages": global_languages,
                "selected_date": selected_date,
                "selected_language": selected_language,
            },
        )

    @app.get("/developers", response_class=HTMLResponse)
    async def developers(request: Request, date: str | None = None, language: str | None = None):
        kind = "developer"
        selected_date = date or _default_date(manifest, kind)
        manifest_kind = manifest.kinds.get(kind)
        dates = manifest_kind.dates if manifest_kind else []
        languages_by_date = manifest_kind.languages_by_date if manifest_kind else {}
        global_languages = manifest_kind.languages if manifest_kind else []
        available = languages_by_date.get(selected_date, global_languages)
        normalized = [("__all__" if value is None else value) for value in available]
        if language:
            selected_language = language
        elif "__all__" in normalized:
            selected_language = "__all__"
        else:
            selected_language = normalized[0] if normalized else "__all__"

        return templates.TemplateResponse(
            "day.html",
            {
                "request": request,
                "kind": kind,
                "title": "Developers",
                "dates": dates,
                "languages_by_date": languages_by_date,
                "global_languages": global_languages,
                "selected_date": selected_date,
                "selected_language": selected_language,
            },
        )

    @app.get("/api/v1/dates")
    async def api_dates(kind: str = Query(...)):
        try:
            dates = query_service.list_dates(kind)
        except InvalidRequestError as exc:
            return JSONResponse(status_code=400, content=_error_response("invalid_kind", str(exc)))
        return {"kind": kind, "dates": dates}

    @app.get("/api/v1/day")
    async def api_day(
        background_tasks: BackgroundTasks,
        kind: str = Query(...),
        date: str = Query(...),
        language: str | None = Query(None),
    ):
        try:
            entries = _cached_day(kind, date, language or "__all__")
        except NotFoundError as exc:
            return JSONResponse(
                status_code=404,
                content=_error_response("date_not_found", str(exc), _date_hint(manifest, kind)),
            )
        prev_date, next_date = _neighbor_dates(kind, date)
        languages = {language or "__all__"}
        languages.add("__all__")
        for target_date in [prev_date, next_date]:
            if not target_date:
                continue
            for lang in languages:
                background_tasks.add_task(_prewarm_day, kind, target_date, lang)
        return {
            "kind": kind,
            "date": date,
            "language": language or "__all__",
            "entries": entries,
        }

    @app.get("/api/v1/top/reappearing")
    async def api_top_reappearing(
        kind: str = Query(...),
        start: str = Query(...),
        end: str = Query(...),
        language: str | None = Query(None),
        presence: str | None = Query(None),
        include_all_languages: str | None = Query(None),
        limit: str | None = Query(None),
    ):
        presence_mode = _parse_presence(presence)
        include_all = _parse_include_all_languages(include_all_languages, default=False)
        limit_value = _parse_limit(limit)
        payload = {
            "kind": kind,
            "start": start,
            "end": end,
            "language": language,
            "presence": presence_mode,
            "include_all_languages": include_all,
            "limit": limit_value,
        }
        results = _cached_toplist(
            "top_reappearing",
            payload,
            lambda: query_service.top_reappearing(
                kind,
                start,
                end,
                language=language,
                presence=presence_mode,
                include_all_languages=include_all,
                limit=limit_value,
            ),
        )
        payload = {
            "kind": kind,
            "start": start,
            "end": end,
            "presence": presence_mode,
            "include_all_languages": include_all,
            "results": results,
        }
        if language is not None:
            payload["language"] = language
        return payload

    @app.get("/api/v1/top/owners")
    async def api_top_owners(
        start: str = Query(...),
        end: str = Query(...),
        language: str | None = Query(None),
        include_all_languages: str | None = Query(None),
        limit: str | None = Query(None),
    ):
        include_all = _parse_include_all_languages(include_all_languages, default=False)
        limit_value = _parse_limit(limit)
        payload = {
            "start": start,
            "end": end,
            "language": language,
            "include_all_languages": include_all,
            "limit": limit_value,
        }
        results = _cached_toplist(
            "top_owners",
            payload,
            lambda: query_service.top_owners(
                start,
                end,
                language=language,
                include_all_languages=include_all,
                limit=limit_value,
            ),
        )
        payload = {
            "start": start,
            "end": end,
            "include_all_languages": include_all,
            "results": results,
        }
        if language is not None:
            payload["language"] = language
        return payload

    @app.get("/api/v1/top/languages")
    async def api_top_languages(
        start: str = Query(...),
        end: str = Query(...),
        kind: str | None = Query(None),
        include_all_languages: str | None = Query(None),
        limit: str | None = Query(None),
    ):
        include_all = _parse_include_all_languages(include_all_languages, default=False)
        limit_value = _parse_limit(limit)
        payload = {
            "start": start,
            "end": end,
            "kind": kind,
            "include_all_languages": include_all,
            "limit": limit_value,
        }
        results = _cached_toplist(
            "top_languages",
            payload,
            lambda: query_service.top_languages(
                start,
                end,
                kind=kind,
                include_all_languages=include_all,
                limit=limit_value,
            ),
        )
        payload = {
            "start": start,
            "end": end,
            "include_all_languages": include_all,
            "results": results,
        }
        if kind:
            payload["kind"] = kind
        return payload

    @app.get("/api/v1/top/streaks")
    async def api_top_streaks(
        kind: str = Query(...),
        start: str = Query(...),
        end: str = Query(...),
        language: str | None = Query(None),
        include_all_languages: str | None = Query(None),
        limit: str | None = Query(None),
    ):
        include_all = _parse_include_all_languages(include_all_languages, default=False)
        limit_value = _parse_limit(limit)
        payload = {
            "kind": kind,
            "start": start,
            "end": end,
            "language": language,
            "include_all_languages": include_all,
            "limit": limit_value,
        }
        results = _cached_toplist(
            "top_streaks",
            payload,
            lambda: query_service.top_streaks(
                kind,
                start,
                end,
                language=language,
                include_all_languages=include_all,
                limit=limit_value,
            ),
        )
        payload = {
            "kind": kind,
            "start": start,
            "end": end,
            "include_all_languages": include_all,
            "results": results,
        }
        if language is not None:
            payload["language"] = language
        return payload

    @app.get("/api/v1/top/newcomers")
    async def api_top_newcomers(
        kind: str = Query(...),
        start: str = Query(...),
        end: str = Query(...),
        language: str | None = Query(None),
        include_all_languages: str | None = Query(None),
        limit: str | None = Query(None),
    ):
        include_all = _parse_include_all_languages(include_all_languages, default=False)
        limit_value = _parse_limit(limit)
        payload = {
            "kind": kind,
            "start": start,
            "end": end,
            "language": language,
            "include_all_languages": include_all,
            "limit": limit_value,
        }
        results = _cached_toplist(
            "top_newcomers",
            payload,
            lambda: query_service.top_newcomers(
                kind,
                start,
                end,
                language=language,
                include_all_languages=include_all,
                limit=limit_value,
            ),
        )
        payload = {
            "kind": kind,
            "start": start,
            "end": end,
            "include_all_languages": include_all,
            "results": results,
        }
        if language is not None:
            payload["language"] = language
        return payload

    return app
