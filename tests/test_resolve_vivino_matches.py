"""Regression tests for resolve_vivino_matches decision logic (real functions, no copies)."""

import argparse
import unittest

from scripts.resolve_vivino_matches import (
    PAGE_NOT_CHECKED,
    Candidate,
    decide,
    gate_on_page_metrics,
    normalize_vivino_url,
    parse_identity,
    rank_candidates,
    vivino_row_has_metrics,
)


def _args(*, require_vivino_metrics: bool = True, auto_accept_best: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        provider="brave",
        min_confidence=0.82,
        min_margin=0.08,
        auto_accept_best=auto_accept_best,
        require_vivino_metrics=require_vivino_metrics,
    )


_ROW = {"name_plat": "2021 G D Vajra - Barolo Albe - Red - 750 ml - Standard Bottle", "year_plat": "2021"}
_IDENTITY = parse_identity(_ROW)


def _candidate(score: float, overlap: int = 2, url: str = "https://www.vivino.com/en/vajra-albe/w/123456") -> Candidate:
    return Candidate(url=url, title="G D Vajra Barolo Albe 2021", query="", provider="brave",
                     score=score, producer_overlap=overlap, year_match=True)


class SameWineDifferentLocaleTests(unittest.TestCase):
    """Bug: /en/ and /US/en/ links to one wine became two candidates, so the margin was ~0."""

    def test_locale_variants_normalise_to_one_url(self) -> None:
        self.assertEqual(
            normalize_vivino_url("https://www.vivino.com/US/en/g-d-vajra-barolo-albe/w/123456?year=2021"),
            normalize_vivino_url("https://www.vivino.com/en/g-d-vajra-barolo-albe/w/123456"),
        )

    def test_locale_variants_rank_as_one_candidate_and_auto_accept(self) -> None:
        hits = [
            {"url": "https://www.vivino.com/en/g-d-vajra-barolo-albe/w/123456", "title": "G D Vajra Barolo Albe 2021"},
            {"url": "https://www.vivino.com/US/en/g-d-vajra-barolo-albe/w/123456?year=2021",
             "title": "G D Vajra Barolo Albe 2021"},
        ]
        ranked = rank_candidates(_IDENTITY, [("q", "brave", hits)])
        self.assertEqual(len(ranked), 1)
        decision, _ = decide(ranked, _args())
        self.assertEqual(decision, "auto_accept")


class PageMetricsGateTests(unittest.TestCase):
    """Bug: the metrics check read the old match's rating, which a new wine never has."""

    def test_new_wine_auto_accepts_on_its_own_page_metrics(self) -> None:
        facts = {"name": "2021 G.D. Vajra Barolo Albe", "rating": 4.2, "num_ratings": 1441, "price": 82.0}
        decision, _ = gate_on_page_metrics(_IDENTITY, "auto_accept", "score=0.95", facts, _args())
        self.assertEqual(decision, "auto_accept")

    def test_page_not_checked_stays_in_review_for_retry(self) -> None:
        decision, reason = gate_on_page_metrics(_IDENTITY, "auto_accept", "score=0.95", None, _args())
        self.assertEqual(decision, "needs_review")
        self.assertIn(PAGE_NOT_CHECKED, reason)

    def test_page_without_rating_or_count_stays_in_review(self) -> None:
        facts = {"name": "2021 G.D. Vajra Barolo Albe", "rating": None, "num_ratings": None}
        decision, _ = gate_on_page_metrics(_IDENTITY, "auto_accept", "score=0.95", facts, _args())
        self.assertEqual(decision, "needs_review")

    def test_page_for_another_producer_stays_in_review(self) -> None:
        facts = {"name": "2021 Giacomo Conterno Barolo", "rating": 4.6, "num_ratings": 900}
        decision, reason = gate_on_page_metrics(_IDENTITY, "auto_accept", "score=0.95", facts, _args())
        self.assertEqual(decision, "needs_review")
        self.assertIn("lacks the producer", reason)

    def test_no_require_metrics_accepts_without_page(self) -> None:
        decision, _ = gate_on_page_metrics(
            _IDENTITY, "auto_accept", "score=0.95", None, _args(require_vivino_metrics=False)
        )
        self.assertEqual(decision, "auto_accept")


class DecisionTests(unittest.TestCase):
    def test_close_second_wine_needs_review(self) -> None:
        ranked = [_candidate(0.90), _candidate(0.86, url="https://www.vivino.com/en/vajra-other/w/999")]
        decision, _ = decide(ranked, _args())
        self.assertEqual(decision, "needs_review")

    def test_zero_producer_overlap_forces_review(self) -> None:
        decision, reason = decide([_candidate(0.99, overlap=0)], _args())
        self.assertEqual(decision, "needs_review")
        self.assertIn("producer", reason)

    def test_no_candidates_is_unmatched(self) -> None:
        self.assertEqual(decide([], _args())[0], "unmatched")


class MetricsDetectionTests(unittest.TestCase):
    def test_url_only_row_is_not_metric_complete(self) -> None:
        self.assertFalse(vivino_row_has_metrics({"vivino_url": "https://www.vivino.com/en/x/w/1", "vivino_rating": ""}))

    def test_rating_or_count_marks_row_metric_complete(self) -> None:
        self.assertTrue(vivino_row_has_metrics({"vivino_rating": "4.1"}))
        self.assertTrue(vivino_row_has_metrics({"vivino_num_ratings": "12"}))


if __name__ == "__main__":
    unittest.main()


class DurableResolverRunTests(unittest.TestCase):
    """Two daily runs with the container disk wiped in between (a Railway redeploy)."""

    def setUp(self) -> None:
        import tempfile
        from pathlib import Path

        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.engine = create_engine(f"sqlite:///{self.root / 'state.db'}")
        self.Session = sessionmaker(bind=self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()
        self.tmp.cleanup()

    def _run(self, search_calls: list, page_fetches: list) -> None:
        import csv
        import shutil
        from unittest.mock import patch

        import app.durable_state as durable_state
        import scripts.resolve_vivino_matches as resolver

        work = self.root / "disk"
        shutil.rmtree(work, ignore_errors=True)  # every run starts on a fresh disk
        work.mkdir()
        comparison = work / "comparison.csv"
        with comparison.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["name_plat", "year_plat", "url_main", "url_plat"])
            writer.writeheader()
            writer.writerow({"name_plat": "2021 G D Vajra - Barolo Albe - Red - 750 ml - Standard Bottle",
                             "year_plat": "2021", "url_main": "", "url_plat": ""})
            writer.writerow({"name_plat": "2020 Obscure Estate - Mystery Cuvee - Red - 750 ml - Standard Bottle",
                             "year_plat": "2020", "url_main": "", "url_plat": ""})
        (work / "vivino.csv").write_text("wine_name,vivino_rating,vivino_num_ratings,vivino_price,vivino_url\n")

        def fake_search(provider, query, max_results, **_):
            search_calls.append(query)
            if "Vajra" in query:
                return [
                    {"url": "https://www.vivino.com/en/g-d-vajra-barolo-albe/w/123456", "title": "G D Vajra Barolo Albe 2021"},
                    {"url": "https://www.vivino.com/US/en/g-d-vajra-barolo-albe/w/123456", "title": "G D Vajra Barolo Albe 2021"},
                ]
            return [{"url": "https://www.vivino.com/en/something-else/w/42", "title": "Unrelated Bottle"}]

        def fake_fetch_facts(fetcher, facts, url, vintage, force=False):
            page_fetches.append(url)
            return {"name": "2021 G.D. Vajra Barolo Albe", "rating": 4.2, "num_ratings": 1441, "price": 82.0}

        class FakeFetcher:
            def finish(self):
                return {}

        args = argparse.Namespace(
            comparison=comparison, vivino=work / "vivino.csv", vivino_overrides=work / "overrides.csv",
            provider="brave", serper_api_key="", google_api_key="", google_cse_id="", brave_api_key="k",
            auto_provider_order="brave", cache_ttl_hours=168.0, max_api_queries=40,
            only_new_unresolved=True, max_results=8, sleep_seconds=0, min_confidence=0.82, min_margin=0.08,
            auto_accept_best=False, require_vivino_metrics=True, limit=0, auto_apply=True,
            output_review=work / "review.csv", output_unmatched=work / "unmatched.csv",
            output_suggestions=work / "suggestions.csv",
        )
        with (
            patch.object(durable_state, "engine", self.engine),
            patch.object(durable_state, "SessionLocal", self.Session),
            patch.object(resolver, "run_search", side_effect=fake_search),
            patch.object(resolver, "fetch_facts", side_effect=fake_fetch_facts),
            patch.object(resolver, "PoliteFetcher", FakeFetcher),
            patch("scripts.llm_utils.load_identity_cache", return_value={}),
        ):
            resolver.resolve_matches(args)

    def test_state_and_matches_survive_redeploy(self) -> None:
        from unittest.mock import patch

        import app.durable_state as durable_state
        from scripts.vivino_overrides import load_durable_overrides

        first_search, first_pages = [], []
        self._run(first_search, first_pages)
        self.assertTrue(first_search)
        self.assertEqual(first_pages, ["https://www.vivino.com/en/g-d-vajra-barolo-albe/w/123456"])

        with patch.object(durable_state, "engine", self.engine), patch.object(durable_state, "SessionLocal", self.Session):
            saved = load_durable_overrides()
        self.assertEqual([row["vivino_url"] for row in saved],
                         ["https://www.vivino.com/en/g-d-vajra-barolo-albe/w/123456"])
        self.assertEqual(saved[0]["vivino_rating"], "4.2")

        second_search, second_pages = [], []
        self._run(second_search, second_pages)
        self.assertEqual(second_search, [], "resolved and already-seen wines must not be searched again")
        self.assertEqual(second_pages, [])
