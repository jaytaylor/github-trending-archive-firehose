from __future__ import annotations

import socket
import threading
import time
from pathlib import Path

import httpx
import uvicorn
from gh_trending_web.app import create_app
from helpers import build_fixture


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _start_server(analytics_root: Path) -> tuple[uvicorn.Server, threading.Thread, str]:
    app = create_app(analytics_root=analytics_root)
    port = _get_free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            response = httpx.get(f"{base_url}/api/v1/dates", params={"kind": "repository"})
            if response.status_code in {200, 400, 404}:
                break
        except httpx.HTTPError:
            time.sleep(0.1)
    return server, thread, base_url


def _stop_server(server: uvicorn.Server, thread: threading.Thread) -> None:
    server.should_exit = True
    thread.join(timeout=5)


def test_e2e_smoke(tmp_path: Path) -> None:
    analytics_root = build_fixture(tmp_path)
    server, thread, base_url = _start_server(analytics_root)
    try:
        day_response = httpx.get(
            f"{base_url}/api/v1/day",
            params={"kind": "repository", "date": "2025-01-01", "language": "python"},
        )
        assert day_response.status_code == 200
        entries = day_response.json()["entries"]
        assert entries[0]["rank"] == 1
        assert entries[0]["full_name"] == "alpha/one"

        reappearing_response = httpx.get(
            f"{base_url}/api/v1/top/reappearing",
            params={
                "kind": "repository",
                "start": "2025-01-01",
                "end": "2025-01-02",
                "presence": "day",
                "include_all_languages": "true",
                "limit": 5,
            },
        )
        assert reappearing_response.status_code == 200
        alpha = next(
            item
            for item in reappearing_response.json()["results"]
            if item["full_name"] == "alpha/one"
        )
        assert alpha["days_present"] == 2

        special_lang_response = httpx.get(
            f"{base_url}/api/v1/day",
            params={"kind": "repository", "date": "2025-01-01", "language": "c++"},
        )
        assert special_lang_response.status_code == 200
    finally:
        _stop_server(server, thread)


def test_e2e_invalid_language(tmp_path: Path) -> None:
    analytics_root = build_fixture(tmp_path)
    server, thread, base_url = _start_server(analytics_root)
    try:
        response = httpx.get(
            f"{base_url}/api/v1/day",
            params={"kind": "repository", "date": "2025-01-01", "language": "badlang"},
        )
        assert response.status_code == 400
    finally:
        _stop_server(server, thread)


def test_e2e_missing_manifest(tmp_path: Path) -> None:
    analytics_root = tmp_path / "analytics"
    analytics_root.mkdir(parents=True, exist_ok=True)
    server, thread, base_url = _start_server(analytics_root)
    try:
        response = httpx.get(
            f"{base_url}/api/v1/dates",
            params={"kind": "repository"},
        )
        assert response.status_code == 404
    finally:
        _stop_server(server, thread)
