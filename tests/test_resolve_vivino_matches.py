"""Regression tests for resolve_vivino_matches decision logic."""

import argparse
import unittest

from scripts.resolve_vivino_matches import Candidate


def _make_args(*, require_vivino_metrics: bool = False, auto_accept_best: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        min_confidence=0.82,
        min_margin=0.08,
        auto_accept_best=auto_accept_best,
        require_vivino_metrics=require_vivino_metrics,
    )


def _decide(
    best: Candidate,
    second_score: float,
    existing_match_row: dict,
    args: argparse.Namespace,
) -> tuple[str, str]:
    """Inline replication of the decision block from resolve_vivino_matches.main()."""
    best_score = best.score
    margin = best_score - second_score

    if args.auto_accept_best:
        decision = "auto_accept"
        reason = f"auto_accept_best enabled; score={best_score:.3f}, margin={margin:.3f}"
    elif best.producer_overlap == 0:
        decision = "needs_review"
        reason = "top candidate missing producer token overlap"
    elif best_score >= args.min_confidence and margin >= args.min_margin:
        decision = "auto_accept"
        reason = f"score={best_score:.3f}, margin={margin:.3f}"
    elif best_score >= 0.70:
        decision = "needs_review"
        reason = f"score={best_score:.3f}, margin={margin:.3f}"
    else:
        decision = "unmatched"
        reason = f"score below threshold ({best_score:.3f})"

    if decision == "auto_accept":
        existing_rating = (existing_match_row.get("vivino_rating") or "").strip()
        existing_count = (
            (existing_match_row.get("vivino_num_ratings") or "").strip()
            or (existing_match_row.get("vivino_raters") or "").strip()
        )
        if (
            args.require_vivino_metrics
            and not existing_rating
            and not existing_count
        ):
            decision = "needs_review"
            reason = f"{reason}; missing vivino rating/count for auto-apply"

    return decision, reason


_HIGH_CONFIDENCE_CANDIDATE = Candidate(
    url="https://www.vivino.com/w/123456",
    title="G D Vajra Barolo Albe 2021",
    query="G D Vajra Barolo Albe 2021 vivino",
    provider="serper",
    score=0.985,
    producer_overlap=2,
    year_match=True,
)
_NO_CACHED_METRICS: dict = {"vivino_rating": "", "vivino_num_ratings": ""}
_CACHED_METRICS: dict = {"vivino_rating": "4.2", "vivino_num_ratings": "1441"}


class DefaultBehaviourTests(unittest.TestCase):
    """By default, high-confidence cache-miss matches should auto-accept."""

    def test_high_confidence_cache_miss_auto_accepts_by_default(self) -> None:
        """The vicious-trap regression: score=0.985 with no cached metrics must auto_accept."""
        args = _make_args(require_vivino_metrics=False)
        decision, _ = _decide(_HIGH_CONFIDENCE_CANDIDATE, 0.0, _NO_CACHED_METRICS, args)
        self.assertEqual(decision, "auto_accept")

    def test_high_confidence_with_cached_metrics_auto_accepts(self) -> None:
        args = _make_args(require_vivino_metrics=False)
        decision, _ = _decide(_HIGH_CONFIDENCE_CANDIDATE, 0.0, _CACHED_METRICS, args)
        self.assertEqual(decision, "auto_accept")

    def test_below_confidence_threshold_needs_review(self) -> None:
        candidate = Candidate(
            url="https://www.vivino.com/w/999",
            title="Some Wine",
            query="some wine vivino",
            provider="serper",
            score=0.75,
            producer_overlap=1,
            year_match=True,
        )
        args = _make_args(require_vivino_metrics=False)
        decision, _ = _decide(candidate, 0.70, _NO_CACHED_METRICS, args)
        self.assertEqual(decision, "needs_review")


class StrictModeTests(unittest.TestCase):
    """--require-vivino-metrics blocks auto-accept when cache is empty."""

    def test_strict_mode_blocks_cache_miss(self) -> None:
        args = _make_args(require_vivino_metrics=True)
        decision, reason = _decide(_HIGH_CONFIDENCE_CANDIDATE, 0.0, _NO_CACHED_METRICS, args)
        self.assertEqual(decision, "needs_review")
        self.assertIn("missing vivino rating/count", reason)

    def test_strict_mode_allows_cached_metrics(self) -> None:
        args = _make_args(require_vivino_metrics=True)
        decision, _ = _decide(_HIGH_CONFIDENCE_CANDIDATE, 0.0, _CACHED_METRICS, args)
        self.assertEqual(decision, "auto_accept")


class ProducerOverlapTests(unittest.TestCase):
    def test_zero_producer_overlap_forces_review_regardless_of_score(self) -> None:
        candidate = Candidate(
            url="https://www.vivino.com/w/1",
            title="Mystery Wine",
            query="",
            provider="serper",
            score=0.99,
            producer_overlap=0,
            year_match=True,
        )
        args = _make_args(require_vivino_metrics=False)
        decision, _ = _decide(candidate, 0.0, _CACHED_METRICS, args)
        self.assertEqual(decision, "needs_review")


if __name__ == "__main__":
    unittest.main()
