import csv
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.models import PriceObservation, WineDeal, WineDealSnapshot
from app.price_history import (
    build_price_history,
    is_seed_replay,
    per_750,
    record_observations,
    run_backfills,
    sg_day,
    volume_ml,
)

NAME = "2020 Jonata - Fenix - Red - 750 ml - Standard Bottle"
GC_URL = "https://grandcruwines.com/products/2020-jonata-fenix"
NOW = datetime(2026, 10, 26, 4, 0, tzinfo=UTC)  # 26 Oct, noon in Singapore


def deal(**overrides):
    base = dict(id=1, wine_name=NAME, volume="750ml", quantity=1, price_platinum=157.0,
                grand_cru_url=GC_URL)
    base.update(overrides)
    return SimpleNamespace(**base)


def obs(source, days_ago, price, url=None, name=NAME, volume=750, quantity=1, origin="daily_check"):
    checked = NOW - timedelta(days=days_ago)
    return {"wine_name": name, "source": source, "price": price, "currency": "SGD",
            "volume_ml": volume, "quantity": quantity, "checked_at": checked,
            "url": url if url is not None else (GC_URL if source == "grand_cru" else None),
            "origin": origin, "ingestion_run_id": None}


class SizeTests(unittest.TestCase):
    def test_volume_parsing(self):
        self.assertEqual(volume_ml("750ml"), 750)
        self.assertEqual(volume_ml("1.5l"), 1500)
        self.assertEqual(volume_ml("375 ml"), 375)
        self.assertEqual(volume_ml("3L"), 3000)
        self.assertIsNone(volume_ml("Magnum"))

    def test_per_750_normalises_half_bottles_magnums_and_bundles(self):
        self.assertEqual(per_750(65.0, 1, 375), 130.0)
        self.assertEqual(per_750(390.0, 1, 1500), 195.0)
        self.assertEqual(per_750(610.0, 3, 750), 203.33)
        self.assertIsNone(per_750(65.0, 1, None))

    def test_singapore_day_boundary(self):
        self.assertEqual(sg_day(datetime(2026, 10, 24, 23, 39, tzinfo=UTC)), date(2026, 10, 25))


class DbTestCase(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def tearDown(self):
        self.engine.dispose()

    def record(self, rows):
        with self.Session() as session:
            record_observations(session, rows)
            session.commit()

    def all_obs(self):
        with self.Session() as session:
            return list(session.scalars(select(PriceObservation).order_by(PriceObservation.observed_on)))

    def history(self, d=None):
        with self.Session() as session:
            return build_price_history(session, [d or deal()], now=NOW)["offers"][0]


class RecordTests(DbTestCase):
    def test_one_row_per_day_latest_check_wins(self):
        early = obs("platinum", 0, 150.0)
        late = dict(obs("platinum", 0, 157.0), checked_at=NOW + timedelta(hours=1))
        self.record([late])
        self.record([early])
        rows = self.all_obs()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].price, 157.0)

    def test_zero_or_missing_prices_are_not_recorded(self):
        self.record([obs("platinum", 0, 0.0), dict(obs("grand_cru", 0, 1.0), price=None)])
        self.assertEqual(self.all_obs(), [])


class BackfillTests(DbTestCase):
    def test_snapshot_backfill_skips_seed_replay_and_runs_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            seed = Path(tmp) / "comparison_summary.csv"
            with seed.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["name_plat", "price_plat"])
                writer.writerow([NAME, "154.00"])
            with self.Session() as session:
                session.add_all([
                    WineDealSnapshot(ingestion_run_id=1, captured_at=NOW - timedelta(days=2), wine_name=NAME,
                                     volume="750ml", quantity=1, price_platinum=157.0, price_grand_cru=139.0,
                                     grand_cru_url=GC_URL),
                    WineDealSnapshot(ingestion_run_id=2, captured_at=NOW - timedelta(days=1), wine_name=NAME,
                                     volume="750ml", quantity=1, price_platinum=154.0),
                ])
                session.commit()
                first = run_backfills(session, seed_comparison=seed, seed_history=Path(tmp) / "none.csv")
                second = run_backfills(session, seed_comparison=seed, seed_history=Path(tmp) / "none.csv")
        self.assertEqual(first["snapshots"], 2)  # run 1: platinum + grand cru; run 2 is a seed replay
        self.assertEqual(second, {})
        self.assertEqual({(o.source, o.price) for o in self.all_obs()}, {("platinum", 157.0), ("grand_cru", 139.0)})

    def test_current_vivino_checks_are_backfilled_with_their_check_date(self):
        checked = NOW - timedelta(days=3)
        with self.Session() as session:
            session.add(WineDeal(wine_name=NAME, volume="750ml", quantity=1, price_platinum=157.0,
                                 vivino_price=180.0, vivino_checked_at=checked, vivino_url="https://www.vivino.com/w/1"))
            session.add(WineDeal(wine_name="Unchecked", volume="750ml", quantity=1, price_platinum=50.0, vivino_price=60.0))
            session.commit()
            run_backfills(session, seed_comparison=Path("/nonexistent"), seed_history=Path("/nonexistent"))
        rows = self.all_obs()
        self.assertEqual([(r.source, r.price, r.observed_on) for r in rows], [("vivino", 180.0, sg_day(checked))])

    def test_seed_replay_needs_an_exact_match(self):
        seed = {("a", 1.0), ("b", 2.0)}
        self.assertTrue(is_seed_replay({("a", 1.0), ("b", 2.0)}, seed))
        self.assertFalse(is_seed_replay({("a", 1.0)}, seed))
        self.assertFalse(is_seed_replay({("a", 1.0)}, set()))

    def test_saved_lists_never_replace_a_daily_check(self):
        self.record([obs("platinum", 5, 157.0)])
        with tempfile.TemporaryDirectory() as tmp:
            history = Path(tmp) / "saved.csv"
            with history.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["commit", "committed_at", "wine_name", "source", "price", "currency",
                                 "volume_ml", "quantity", "url"])
                writer.writerow(["abc", (NOW - timedelta(days=5)).isoformat(), NAME, "platinum", "1.0", "SGD", "750", "1", ""])
                writer.writerow(["abc", (NOW - timedelta(days=200)).isoformat(), NAME, "platinum", "154.0", "SGD", "750", "1", ""])
            with self.Session() as session:
                run_backfills(session, seed_comparison=Path(tmp) / "none.csv", seed_history=history)
        rows = self.all_obs()
        self.assertEqual([(r.price, r.origin) for r in rows], [(154.0, "saved_list"), (157.0, "daily_check")])


class SeriesTests(DbTestCase):
    def test_segments_merge_same_price_and_break_on_missing_days(self):
        self.record([obs("platinum", d, 157.0) for d in (10, 9, 8, 5, 4)] + [obs("platinum", 3, 160.0)])
        platinum = self.history()["sources"][0]
        self.assertEqual(platinum["source"], "platinum")
        self.assertEqual([(s["start"], s["end"], s["price"]) for s in platinum["segments"]], [
            ("2026-10-16", "2026-10-18", 157.0),
            ("2026-10-21", "2026-10-22", 157.0),
            ("2026-10-23", "2026-10-23", 160.0),
        ])

    def test_grand_cru_history_only_for_the_current_product_page(self):
        self.record([obs("platinum", 1, 157.0), obs("grand_cru", 2, 99.0, url="https://grandcruwines.com/products/other"),
                     obs("grand_cru", 1, 139.0)])
        gc = next(s for s in self.history()["sources"] if s["source"] == "grand_cru")
        self.assertEqual([s["price"] for s in gc["segments"]], [139.0])

    def test_vivino_is_drawn_as_dated_checks_normalised_to_750(self):
        d = deal(volume="375ml", price_platinum=65.0)
        self.record([obs("platinum", 0, 65.0, volume=375), obs("vivino", 3, 70.0, volume=375)])
        vivino = next(s for s in self.history(d)["sources"] if s["source"] == "vivino")
        self.assertEqual(vivino["kind"], "checks")
        self.assertEqual(vivino["points"][0]["price_per_750"], 140.0)


class VerdictTests(DbTestCase):
    def texts(self, d=None):
        return [v["text"] for v in self.history(d)["verdicts"]]

    def test_unchanged_needs_two_weeks_of_history(self):
        self.record([obs("platinum", d, 157.0) for d in range(0, 10)])
        self.assertEqual(self.texts(), [])
        self.record([obs("platinum", 20, 157.0)])
        self.assertEqual(self.texts(), ["Same Platinum price at every check since 6 Oct"])

    def test_lowest_recorded_price(self):
        self.record([obs("platinum", 30, 170.0), obs("platinum", 10, 165.0), obs("platinum", 0, 157.0)])
        self.assertIn("Lowest Platinum price we have recorded since 26 Sep", self.texts())

    def test_up_since_last_different_price(self):
        self.record([obs("platinum", 3, 154.0), obs("platinum", 2, 154.0), obs("platinum", 0, 157.0)])
        self.assertIn("Up S$3 since 24 Oct", self.texts())

    def test_cross_source_below_and_above(self):
        self.record([obs("platinum", 0, 157.0), obs("grand_cru", 0, 139.0)])
        self.assertIn("S$18 cheaper at Grand Cru", self.texts())
        self.record([obs("grand_cru", 0, 170.0), obs("vivino", 2, 180.0)])
        self.assertIn("S$13 below the cheapest other price we track (Grand Cru)", self.texts())

    def test_stale_comparison_prices_are_ignored(self):
        self.record([obs("platinum", 0, 157.0), obs("grand_cru", 10, 139.0), obs("vivino", 45, 100.0)])
        self.assertEqual(self.texts(), [])

    def test_bundle_differences_are_in_listing_terms(self):
        d = deal(quantity=3, price_platinum=610.0)
        self.record([obs("platinum", 0, 610.0, quantity=3), obs("vivino", 1, 630.0, quantity=3)])
        self.assertIn("S$20 below the Vivino market price", self.texts(d))


if __name__ == "__main__":
    unittest.main()
