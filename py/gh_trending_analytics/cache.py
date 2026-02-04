from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any


@dataclass
class CacheEntry:
    value: Any
    expires_at: float


@dataclass
class CacheStats:
    hits: int = 0
    misses: int = 0
    sets: int = 0
    evictions: int = 0
    expirations: int = 0
    prewarm_success: int = 0
    prewarm_failure: int = 0


class ResultCache:
    def __init__(self, *, max_size: int = 1024, default_ttl: float = 300.0) -> None:
        self._data: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_size = max_size
        self._default_ttl = default_ttl
        self.stats = CacheStats()

    def get(self, key: str) -> Any | None:
        entry = self._data.get(key)
        if entry is None:
            self.stats.misses += 1
            return None
        now = time.time()
        if entry.expires_at <= now:
            self._data.pop(key, None)
            self.stats.misses += 1
            self.stats.expirations += 1
            return None
        self._data.move_to_end(key)
        self.stats.hits += 1
        return entry.value

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        ttl_value = self._default_ttl if ttl is None else ttl
        expires_at = time.time() + ttl_value
        if key in self._data:
            self._data.pop(key, None)
        self._data[key] = CacheEntry(value=value, expires_at=expires_at)
        self._data.move_to_end(key)
        self.stats.sets += 1
        self._evict_if_needed()

    def _evict_if_needed(self) -> None:
        while len(self._data) > self._max_size:
            self._data.popitem(last=False)
            self.stats.evictions += 1

    def size(self) -> int:
        return len(self._data)

    def clear(self) -> None:
        self._data.clear()

    def keys(self) -> list[str]:
        return list(self._data.keys())
