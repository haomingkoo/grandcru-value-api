"""Polite, budgeted Vivino wine-page fetcher.

Every Vivino page request in this repo goes through PoliteFetcher: honest
User-Agent, robots.txt, wine pages only, a daily cap shared across runs,
a jittered delay, and a stop-and-back-off on 403/429. Budget, block state
and the per-wine fact cache live in the database (app_state).
"""

from __future__ import annotations

import json
import os
import random
import re
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

USER_AGENT = (
    "Mozilla/5.0 (compatible; MinMaxWine/1.0; +https://wine.kooexperience.com/privacy; "
    "privacy@kooexperience.com)"
)
ROBOTS_AGENT = "MinMaxWine"
ROBOTS_URL = "https://www.vivino.com/robots.txt"
FETCH_STATE_KEY = "vivino_fetch"
FACTS_KEY = "vivino_facts"
REFRESH_AFTER = timedelta(days=7)
DEFAULT_BACKOFF = timedelta(hours=24)
MAX_BACKOFF = timedelta(days=7)

_WINE_PATH_RE = re.compile(
    r"^/(?:(?:[A-Za-z]{2}/)?[a-z]{2}(?:-[A-Za-z]{2})?/)?(?P<slug>[^/]+)/w/(?P<id>\d+)/?$"
)
_YEAR_RE = re.compile(r"(19|20)\d{2}")
_LD_JSON_RE = re.compile(
    r'<script[^>]*type="application/ld\+json"[^>]*>(?P<body>.*?)</script>', re.DOTALL
)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def canonical_wine_url(url: str | None) -> str | None:
    """One URL per Vivino wine: locale and query dropped, so /en/ and /US/en/ compare equal."""
    if not url:
        return None
    parsed = urlparse(url.strip())
    if not (parsed.netloc or "").endswith("vivino.com"):
        return None
    match = _WINE_PATH_RE.match(parsed.path or "")
    if not match:
        return None
    return f"https://www.vivino.com/en/{match.group('slug')}/w/{match.group('id')}"


def wine_id(url: str | None) -> str | None:
    canonical = canonical_wine_url(url)
    return canonical.rsplit("/", 1)[-1] if canonical else None


def page_url(url: str, vintage: int | str | None) -> str | None:
    canonical = canonical_wine_url(url)
    if not canonical:
        return None
    year = str(vintage or "").strip()
    return f"{canonical}?year={year}" if _YEAR_RE.fullmatch(year) else canonical


def fact_key(url: str | None, vintage: int | str | None) -> str | None:
    wid = wine_id(url)
    if not wid:
        return None
    year = str(vintage or "").strip()
    return f"{wid}|{year if _YEAR_RE.fullmatch(year) else 'nv'}"


def parse_vivino_facts(html: str) -> dict[str, Any]:
    """Facts only: name, rating, rating count, SGD price. No review text, no images."""
    for block in _LD_JSON_RE.finditer(html or ""):
        try:
            data = json.loads(block.group("body"))
        except json.JSONDecodeError:
            continue
        # Wines with no ratings or offers are published as a bare WebPage.
        if not isinstance(data, dict) or data.get("@type") not in ("Product", "WebPage"):
            continue
        facts: dict[str, Any] = {"name": str(data.get("name") or "").strip()[:255]}
        rating = data.get("aggregateRating") or {}
        try:
            facts["rating"] = round(float(rating.get("ratingValue")), 1)
        except (TypeError, ValueError):
            facts["rating"] = None
        try:
            facts["num_ratings"] = int(str(rating.get("ratingCount")).replace(",", ""))
        except (TypeError, ValueError):
            facts["num_ratings"] = None
        offers = data.get("offers") or {}
        if isinstance(offers, list):
            offers = next((o for o in offers if isinstance(o, dict)), {})
        facts["price"] = None
        facts["currency"] = str(offers.get("priceCurrency") or "")
        try:
            price = float(offers.get("price") or offers.get("lowPrice"))
        except (TypeError, ValueError):
            price = None
        if price is not None and facts["currency"] == "SGD" and 5 <= price <= 50000:
            facts["price"] = round(price, 2)
        return facts
    return {}


def is_fact_fresh(fact: dict[str, Any] | None, now: datetime) -> bool:
    if not fact or not fact.get("checked_at"):
        return False
    try:
        checked = datetime.fromisoformat(fact["checked_at"])
    except ValueError:
        return False
    return now - checked < REFRESH_AFTER


class VivinoBlocked(Exception):
    """Vivino answered 403/429; stop fetching for this run."""


class PoliteFetcher:
    def __init__(
        self,
        *,
        load: Callable[[str, Any], Any] | None = None,
        save: Callable[[str, Any], None] | None = None,
        opener: Callable[..., Any] = urlopen,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        daily_cap: int | None = None,
        min_delay: float | None = None,
        jitter: float | None = None,
    ) -> None:
        if load is None or save is None:
            from app.durable_state import load_state, save_state

            load = load or load_state
            save = save or save_state
        self._load = load
        self._save = save
        self._opener = opener
        self._sleep = sleep
        self._now = now
        self.daily_cap = int(daily_cap if daily_cap is not None else _env_float("VIVINO_DAILY_FETCH_CAP", 80))
        self.min_delay = min_delay if min_delay is not None else _env_float("VIVINO_MIN_DELAY_SECONDS", 6.0)
        self.jitter = jitter if jitter is not None else _env_float("VIVINO_DELAY_JITTER_SECONDS", 4.0)
        self.state: dict[str, Any] = dict(load(FETCH_STATE_KEY, {}) or {})
        self._robots: RobotFileParser | None = None
        self._last_request: float | None = None
        self.attempted = 0
        self.ok = 0
        self.failed = 0
        self.disallowed = 0
        self.blocked = False

    # -- budget and backoff -------------------------------------------------

    def _today(self) -> str:
        return self._now().date().isoformat()

    def fetches_today(self) -> int:
        return int(self.state.get("fetches_today") or 0) if self.state.get("day") == self._today() else 0

    def remaining_budget(self) -> int:
        return max(0, self.daily_cap - self.fetches_today())

    def in_backoff(self) -> bool:
        until = self.state.get("backoff_until")
        if not until:
            return False
        try:
            return self._now() < datetime.fromisoformat(until)
        except ValueError:
            return False

    def can_fetch_more(self) -> bool:
        return not self.blocked and not self.in_backoff() and self.remaining_budget() > 0

    def _spend(self) -> None:
        today = self._today()
        if self.state.get("day") != today:
            self.state["day"] = today
            self.state["fetches_today"] = 0
        self.state["fetches_today"] = int(self.state.get("fetches_today") or 0) + 1
        self._save(FETCH_STATE_KEY, self.state)

    def _wait_turn(self) -> None:
        if self._last_request is not None:
            gap = self.min_delay + random.uniform(0, max(self.jitter, 0.0))
            remaining = gap - (time.monotonic() - self._last_request)
            if remaining > 0:
                self._sleep(remaining)
        self._last_request = time.monotonic()

    def _mark_blocked(self, code: int, retry_after: str | None) -> None:
        now = self._now()
        strikes = int(self.state.get("block_strikes") or 0) + 1
        backoff = min(DEFAULT_BACKOFF * (2 ** (strikes - 1)), MAX_BACKOFF)
        if retry_after and retry_after.isdigit():
            backoff = max(backoff, timedelta(seconds=int(retry_after)))
        self.state.update(
            {
                "blocked_since": self.state.get("blocked_since") or now.isoformat(),
                "block_strikes": strikes,
                "backoff_until": (now + backoff).isoformat(),
                "last_http_status": code,
            }
        )
        self.blocked = True
        self._save(FETCH_STATE_KEY, self.state)

    # -- requests -------------------------------------------------------------

    def _request(self, url: str) -> str:
        self._wait_turn()
        self._spend()
        request = Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en",
                "From": "privacy@kooexperience.com",
            },
        )
        try:
            with self._opener(request, timeout=20) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                return response.read().decode(charset, errors="replace")
        except HTTPError as exc:
            if exc.code in (403, 429):
                self._mark_blocked(exc.code, exc.headers.get("Retry-After") if exc.headers else None)
                raise VivinoBlocked(f"HTTP {exc.code} from {urlparse(url).path}") from exc
            raise

    def _robots_allows(self, url: str) -> bool:
        if self._robots is None:
            parser = RobotFileParser()
            try:
                parser.parse(self._request(ROBOTS_URL).splitlines())
            except VivinoBlocked:
                raise
            except (HTTPError, URLError, TimeoutError, OSError):
                parser.disallow_all = True
            self._robots = parser
        return self._robots.can_fetch(ROBOTS_AGENT, url)

    def fetch(self, url: str) -> str | None:
        """Return page HTML, or None when not allowed, out of budget or failed."""
        if canonical_wine_url(url) is None:
            self.disallowed += 1
            return None
        if not self.can_fetch_more():
            return None
        if not self._robots_allows(url):
            self.disallowed += 1
            return None
        if not self.can_fetch_more():
            return None
        self.attempted += 1
        try:
            html = self._request(url)
        except VivinoBlocked:
            self.failed += 1
            raise
        except (HTTPError, URLError, TimeoutError, OSError):
            self.failed += 1
            return None
        self.ok += 1
        now = self._now().isoformat()
        self.state.update({"last_success_at": now, "blocked_since": None, "backoff_until": None, "block_strikes": 0})
        self.state.pop("last_http_status", None)
        self._save(FETCH_STATE_KEY, self.state)
        return html

    def record_unreadable(self) -> None:
        """A 200 with no product data (e.g. a challenge page) counts as a failure."""
        self.ok -= 1
        self.failed += 1

    def finish(self) -> dict[str, Any]:
        summary = {
            "at": self._now().isoformat(),
            "attempted": self.attempted,
            "ok": self.ok,
            "failed": self.failed,
            "disallowed": self.disallowed,
            "blocked": self.blocked,
        }
        if self.attempted or self.blocked:
            self.state["last_run"] = summary
            self._save(FETCH_STATE_KEY, self.state)
        return summary


def fetch_facts(
    fetcher: PoliteFetcher,
    facts: dict[str, dict[str, Any]],
    url: str,
    vintage: int | str | None,
    *,
    force: bool = False,
) -> dict[str, Any] | None:
    """Cached facts for a wine/vintage, fetching at most once a week. Raises VivinoBlocked."""
    key = fact_key(url, vintage)
    target = page_url(url, vintage)
    if not key or not target:
        return None
    cached = facts.get(key)
    if not force and is_fact_fresh(cached, fetcher._now()):
        return cached
    html = fetcher.fetch(target)
    if html is None:
        return cached
    parsed = parse_vivino_facts(html)
    if not parsed:
        fetcher.record_unreadable()
        return cached
    fact = {
        "url": canonical_wine_url(url),
        "name": parsed.get("name") or "",
        "rating": parsed.get("rating"),
        "num_ratings": parsed.get("num_ratings"),
        "price": parsed.get("price"),
        "currency": parsed.get("currency") or "",
        "checked_at": fetcher._now().isoformat(),
    }
    facts[key] = fact
    return fact
