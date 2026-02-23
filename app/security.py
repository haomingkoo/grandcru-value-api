from collections import defaultdict, deque
from collections.abc import Iterable
from dataclasses import dataclass
from threading import Lock
from time import time


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    remaining: int
    reset_seconds: int


class InMemoryRateLimiter:
    """Simple per-IP sliding window limiter."""

    def __init__(self, requests_per_minute: int):
        if requests_per_minute < 1:
            raise ValueError("requests_per_minute must be >= 1")
        self._limit = requests_per_minute
        self._window_seconds = 60
        self._buckets: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    @property
    def limit(self) -> int:
        return self._limit

    def check(self, key: str) -> RateLimitResult:
        now = time()
        window_start = now - self._window_seconds

        with self._lock:
            bucket = self._buckets[key]
            while bucket and bucket[0] < window_start:
                bucket.popleft()

            if len(bucket) >= self._limit:
                reset_seconds = max(int(self._window_seconds - (now - bucket[0])), 1)
                return RateLimitResult(allowed=False, remaining=0, reset_seconds=reset_seconds)

            bucket.append(now)
            remaining = max(self._limit - len(bucket), 0)
            reset_seconds = max(int(self._window_seconds - (now - bucket[0])), 1)
            return RateLimitResult(allowed=True, remaining=remaining, reset_seconds=reset_seconds)


def parse_exempt_paths(raw: str) -> set[str]:
    return {part.strip() for part in raw.split(",") if part.strip()}


def resolve_client_ip(
    client_host: str | None,
    x_forwarded_for: str | None,
    x_real_ip: str | None,
) -> str:
    if x_forwarded_for:
        # X-Forwarded-For: client, proxy1, proxy2
        first = x_forwarded_for.split(",")[0].strip()
        if first:
            return first
    if x_real_ip and x_real_ip.strip():
        return x_real_ip.strip()
    return client_host or "unknown"


def is_exempt_path(path: str, exempt_paths: Iterable[str]) -> bool:
    return path in exempt_paths
