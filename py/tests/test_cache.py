from __future__ import annotations

import time

from gh_trending_analytics.cache import ResultCache
from gh_trending_analytics.utils import CacheKey


def test_cache_ttl_expiry() -> None:
    cache = ResultCache(max_size=2, default_ttl=0.001)
    cache.set("alpha", {"value": 1})
    time.sleep(0.01)
    assert cache.get("alpha") is None
    assert cache.stats.expirations == 1


def test_cache_key_collision() -> None:
    key_a = CacheKey("day", {"kind": "repository", "date": "2025-01-01"}).as_str()
    key_b = CacheKey("day", {"kind": "repository", "date": "2025-01-02"}).as_str()
    assert key_a != key_b
