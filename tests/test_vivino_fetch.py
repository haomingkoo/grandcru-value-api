"""Polite Vivino fetching: robots, budget, delay, cache, backoff, loud status."""

import io
import json
import unittest
from datetime import UTC, datetime, timedelta
from email.message import Message
from urllib.error import HTTPError

from app.vivino_status import summarize_vivino_status
from scripts.vivino_fetch import (
    USER_AGENT,
    PoliteFetcher,
    VivinoBlocked,
    canonical_wine_url,
    fact_key,
    fetch_facts,
    parse_vivino_facts,
)

ROBOTS = "User-Agent: *\nDisallow: /prices/\nDisallow: /wine-searcher/\nDisallow: /reviews/\n"
NOW = datetime(2026, 10, 26, 4, 0, tzinfo=UTC)


def _page(price="51", currency="SGD", rating="4.0", count="2394", name="2021 Miraval Rose") -> str:
    product = {
        "@type": "Product",
        "name": name,
        "image": "https://images.vivino.com/x.png",
        "review": [{"reviewBody": "Loved it, tastes like summer"}],
        "offers": {"@type": "Offer", "priceCurrency": currency, "price": price},
        "aggregateRating": {"@type": "AggregateRating", "ratingValue": rating, "ratingCount": count},
    }
    return f'<html><script type="application/ld+json">{json.dumps(product)}</script></html>'


class _Response(io.BytesIO):
    def __init__(self, body: str):
        super().__init__(body.encode("utf-8"))
        self.headers = Message()
        self.headers["Content-Type"] = "text/html; charset=utf-8"

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Vivino:
    """Fake opener: records requests, serves robots.txt and wine pages."""

    def __init__(self, page: str | None = None, fail_with: int | None = None):
        self.page = page or _page()
        self.fail_with = fail_with
        self.requests: list = []

    def __call__(self, request, timeout=None):
        self.requests.append(request)
        if request.full_url.endswith("/robots.txt"):
            return _Response(ROBOTS)
        if self.fail_with:
            headers = Message()
            raise HTTPError(request.full_url, self.fail_with, "blocked", headers, None)
        return _Response(self.page)


class _Store(dict):
    def load(self, key, default=None):
        return json.loads(json.dumps(self.get(key, default)))

    def save(self, key, value):
        self[key] = json.loads(json.dumps(value))


def _fetcher(store: _Store, opener: _Vivino, *, now=NOW, cap=80, sleeps=None) -> PoliteFetcher:
    return PoliteFetcher(
        load=store.load, save=store.save, opener=opener,
        sleep=(sleeps.append if sleeps is not None else (lambda _s: None)),
        now=lambda: now, daily_cap=cap, min_delay=6.0, jitter=4.0,
    )


WINE = "https://www.vivino.com/SG/en/miraval-cotes-de-provence-rose/w/2638097"


class UrlTests(unittest.TestCase):
    def test_locales_and_query_collapse_to_one_url(self) -> None:
        expected = "https://www.vivino.com/en/miraval-cotes-de-provence-rose/w/2638097"
        for url in (WINE, WINE.replace("/SG/en/", "/en/"), WINE.replace("/SG/en/", "/US/en/") + "?year=2021"):
            self.assertEqual(canonical_wine_url(url), expected)

    def test_non_wine_pages_are_rejected(self) -> None:
        for url in ("https://www.vivino.com/prices/123", "https://www.vivino.com/en/search/wines?q=x",
                    "https://example.com/en/x/w/1"):
            self.assertIsNone(canonical_wine_url(url))


class ParseTests(unittest.TestCase):
    def test_keeps_facts_only(self) -> None:
        facts = parse_vivino_facts(_page())
        self.assertEqual(facts, {"name": "2021 Miraval Rose", "rating": 4.0, "num_ratings": 2394,
                                 "price": 51.0, "currency": "SGD"})

    def test_unrated_wine_webpage_is_readable_without_numbers(self) -> None:
        page = '<script type="application/ld+json">{"@type":"WebPage","name":"2022 Quiet Wine","image":["x"]}</script>'
        self.assertEqual(parse_vivino_facts(page), {"name": "2022 Quiet Wine", "rating": None, "num_ratings": None,
                                                    "price": None, "currency": ""})

    def test_non_sgd_price_is_dropped(self) -> None:
        self.assertIsNone(parse_vivino_facts(_page(currency="USD"))["price"])


class PoliteFetcherTests(unittest.TestCase):
    def test_honest_user_agent_and_robots_disallow(self) -> None:
        store, vivino = _Store(), _Vivino()
        fetcher = _fetcher(store, vivino)
        self.assertIsNone(fetcher.fetch("https://www.vivino.com/prices/2638097"))
        self.assertIsNotNone(fetcher.fetch(WINE))
        agents = {r.get_header("User-agent") for r in vivino.requests}
        self.assertEqual(agents, {USER_AGENT})
        self.assertIn("wine.kooexperience.com", USER_AGENT)
        self.assertIn("privacy@kooexperience.com", USER_AGENT)
        self.assertFalse(any("/prices/" in r.full_url for r in vivino.requests))

    def test_delay_between_requests_is_at_least_the_minimum(self) -> None:
        store, vivino, sleeps = _Store(), _Vivino(), []
        fetcher = _fetcher(store, vivino, sleeps=sleeps)
        for wine_id in (1, 2, 3):
            fetcher.fetch(f"https://www.vivino.com/en/w-{wine_id}/w/{wine_id}")
        self.assertEqual(len(sleeps), 3)  # robots -> page 1 -> page 2 -> page 3
        self.assertTrue(all(5.9 <= s <= 10.0 for s in sleeps), sleeps)

    def test_daily_cap_holds_across_runs(self) -> None:
        store, vivino = _Store(), _Vivino()
        first = _fetcher(store, vivino, cap=3)
        self.assertIsNotNone(first.fetch("https://www.vivino.com/en/a/w/1"))  # robots + 1
        self.assertIsNotNone(first.fetch("https://www.vivino.com/en/b/w/2"))  # 3 used
        second = _fetcher(store, vivino, cap=3)
        self.assertIsNone(second.fetch("https://www.vivino.com/en/c/w/3"))
        self.assertEqual(len(vivino.requests), 3)
        tomorrow = _fetcher(store, vivino, cap=3, now=NOW + timedelta(days=1))
        self.assertIsNotNone(tomorrow.fetch("https://www.vivino.com/en/c/w/3"))

    def test_403_stops_backs_off_and_is_loud(self) -> None:
        store = _Store()
        fetcher = _fetcher(store, _Vivino(fail_with=403))
        with self.assertRaises(VivinoBlocked):
            fetcher.fetch(WINE)
        fetcher.finish()
        self.assertFalse(fetcher.can_fetch_more())

        later = _Vivino()
        retry = _fetcher(store, later, now=NOW + timedelta(hours=3))
        self.assertIsNone(retry.fetch(WINE))
        self.assertEqual(later.requests, [])

        status = summarize_vivino_status(store["vivino_fetch"], NOW + timedelta(hours=3))
        self.assertEqual(status["vivino_status"], f"blocked since {NOW.isoformat()}")
        self.assertFalse(status["vivino_ok"])

    def test_429_backoff_lifts_and_success_clears_the_block(self) -> None:
        store = _Store()
        with self.assertRaises(VivinoBlocked):
            _fetcher(store, _Vivino(fail_with=429)).fetch(WINE)
        recovered = _fetcher(store, _Vivino(), now=NOW + timedelta(hours=25))
        self.assertIsNotNone(recovered.fetch(WINE))
        recovered.finish()
        self.assertTrue(summarize_vivino_status(store["vivino_fetch"], NOW + timedelta(hours=25))["vivino_ok"])


class FactCacheTests(unittest.TestCase):
    def test_refetch_at_most_weekly_or_on_vintage_change(self) -> None:
        store, vivino, facts = _Store(), _Vivino(), {}
        fetcher = _fetcher(store, vivino)
        fact = fetch_facts(fetcher, facts, WINE, "2021")
        self.assertEqual(fact["price"], 51.0)
        self.assertIn("?year=2021", vivino.requests[-1].full_url)
        pages = len(vivino.requests)

        six_days = _fetcher(store, vivino, now=NOW + timedelta(days=6))
        fetch_facts(six_days, facts, WINE.replace("/SG/en/", "/US/en/"), "2021")
        self.assertEqual(len(vivino.requests), pages)

        fetch_facts(six_days, facts, WINE, "2022")
        self.assertIn("?year=2022", vivino.requests[-1].full_url)
        self.assertIn(fact_key(WINE, "2022"), facts)

        eight_days = _fetcher(store, vivino, now=NOW + timedelta(days=8))
        fetch_facts(eight_days, facts, WINE, "2021")
        self.assertIn("?year=2021", vivino.requests[-1].full_url)

    def test_challenge_page_counts_as_failure_and_keeps_old_facts(self) -> None:
        store, facts = _Store(), {}
        fetch_facts(_fetcher(store, _Vivino()), facts, WINE, "2021")
        old = dict(facts[fact_key(WINE, "2021")])
        later = _fetcher(store, _Vivino(page="<html>Are you human?</html>"), now=NOW + timedelta(days=8))
        fetch_facts(later, facts, WINE, "2021")
        self.assertEqual(facts[fact_key(WINE, "2021")], old)
        self.assertEqual((later.ok, later.failed), (0, 1))


class StatusTests(unittest.TestCase):
    def test_low_success_rate_is_degraded(self) -> None:
        state = {"last_success_at": NOW.isoformat(),
                 "last_run": {"at": NOW.isoformat(), "attempted": 10, "ok": 3}}
        status = summarize_vivino_status(state, NOW)
        self.assertTrue(status["vivino_status"].startswith("degraded since"))
        self.assertFalse(status["vivino_ok"])

    def test_old_last_success_is_stale(self) -> None:
        state = {"last_success_at": (NOW - timedelta(days=11)).isoformat()}
        self.assertTrue(summarize_vivino_status(state, NOW)["vivino_status"].startswith("stale since"))

    def test_healthy(self) -> None:
        state = {"last_success_at": NOW.isoformat(), "last_run": {"attempted": 10, "ok": 10}}
        self.assertEqual(summarize_vivino_status(state, NOW)["vivino_status"], "ok")


if __name__ == "__main__":
    unittest.main()
