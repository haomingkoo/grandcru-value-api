import csv
from dataclasses import replace
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import app.database as database_module
import app.durable_state as durable_state_module
import scripts.import_wine_data as import_wine_data_module
import scripts.validate_wine_completeness as validate_module
from app.database import Base
from app.models import IngestionRun, WineDeal, WineDealSnapshot
from scripts.import_wine_data import (
    _db_has_fresh_data,
    _resolve_vivino_price_to_listing,
    _scale_vivino_price_to_listing,
    default_skip_if_fresh_hours,
    import_data,
)


class ImportWineDataTests(unittest.TestCase):
    def test_default_skip_if_fresh_protects_startup_imports(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(default_skip_if_fresh_hours(), 20.0)

    def test_default_skip_if_fresh_can_be_overridden(self) -> None:
        with patch.dict("os.environ", {"IMPORT_SKIP_IF_FRESH_HOURS": "0"}, clear=True):
            self.assertEqual(default_skip_if_fresh_hours(), 0.0)

    def test_fresh_data_check_uses_latest_successful_finished_ingestion(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = create_engine(
                f"sqlite:///{Path(tmpdir) / 'freshness.sqlite'}",
                connect_args={"check_same_thread": False},
            )
            Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
            now = datetime.now(UTC)

            with (
                patch.object(import_wine_data_module, "engine", engine),
                patch.object(import_wine_data_module, "SessionLocal", Session),
            ):
                Base.metadata.create_all(bind=engine)
                with Session() as session:
                    session.add(
                        IngestionRun(
                            started_at=now - timedelta(hours=1),
                            finished_at=now - timedelta(hours=1),
                            status="success",
                            comparison_rows=62,
                            vivino_rows=368,
                            merged_rows=62,
                        )
                    )
                    session.add(
                        IngestionRun(
                            started_at=now,
                            finished_at=None,
                            status="started",
                            comparison_rows=0,
                            vivino_rows=0,
                            merged_rows=0,
                        )
                    )
                    session.commit()

                self.assertTrue(_db_has_fresh_data(max_age_hours=20))
            engine.dispose()

    def test_scale_vivino_price_to_listing_scales_bundle(self) -> None:
        self.assertEqual(_scale_vivino_price_to_listing(210.0, 3, "750ml"), 630.0)

    def test_scale_vivino_price_to_listing_scales_magnum(self) -> None:
        self.assertEqual(_scale_vivino_price_to_listing(75.0, 1, "1.5l"), 150.0)

    def test_override_price_keeps_listing_total_when_scaled_value_is_implausible(self) -> None:
        adjusted = _resolve_vivino_price_to_listing(
            527.67,
            6,
            "750ml",
            price_platinum=630.0,
            price_grand_cru=660.0,
            source="override",
            vivino_url="https://www.vivino.com/w/1",
        )

        self.assertEqual(adjusted, 527.67)

    def test_override_price_still_scales_when_bottle_price_matches_retail_anchor(self) -> None:
        adjusted = _resolve_vivino_price_to_listing(
            210.0,
            3,
            "750ml",
            price_platinum=600.0,
            price_grand_cru=630.0,
            source="override",
            vivino_url="https://www.vivino.com/w/1",
        )

        self.assertEqual(adjusted, 630.0)

    def test_base_price_always_scales_to_listing(self) -> None:
        adjusted = _resolve_vivino_price_to_listing(
            210.0,
            3,
            "750ml",
            price_platinum=600.0,
            price_grand_cru=630.0,
            source="base",
            vivino_url="https://www.vivino.com/w/1",
        )

        self.assertEqual(adjusted, 630.0)

    def test_override_price_drops_high_outlier_after_normalization(self) -> None:
        adjusted = _resolve_vivino_price_to_listing(
            1199.95,
            3,
            "750ml",
            price_platinum=600.0,
            price_grand_cru=555.0,
            source="override",
            vivino_url="https://www.vivino.com/w/1",
        )

        self.assertIsNone(adjusted)

    def test_override_price_drops_low_outlier_after_normalization(self) -> None:
        adjusted = _resolve_vivino_price_to_listing(
            75.0,
            1,
            "1.5l",
            price_platinum=380.0,
            price_grand_cru=445.0,
            source="override",
            vivino_url="https://www.vivino.com/w/1",
        )

        self.assertIsNone(adjusted)

    def test_override_price_drops_standard_bottle_outlier(self) -> None:
        adjusted = _resolve_vivino_price_to_listing(
            42.94,
            1,
            "750ml",
            price_platinum=110.0,
            price_grand_cru=113.0,
            source="override",
            vivino_url="https://www.vivino.com/w/1",
        )

        self.assertIsNone(adjusted)

    def test_price_cleared_when_no_vivino_link(self) -> None:
        adjusted = _resolve_vivino_price_to_listing(
            100.0,
            1,
            "750ml",
            price_platinum=100.0,
            price_grand_cru=100.0,
            source="override",
            vivino_url=None,
        )

        self.assertIsNone(adjusted)

    def test_price_kept_when_vivino_link_present(self) -> None:
        adjusted = _resolve_vivino_price_to_listing(
            100.0,
            1,
            "750ml",
            price_platinum=100.0,
            price_grand_cru=100.0,
            source="override",
            vivino_url="https://www.vivino.com/w/12345",
        )

        self.assertEqual(adjusted, 100.0)


class ImportWineDataPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.engine = create_engine(
            f"sqlite:///{self.root / 'wines.sqlite'}",
            connect_args={"check_same_thread": False},
        )
        self.Session = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

        self.comparison_path = self.root / "comparison_summary.csv"
        self.vivino_path = self.root / "vivino_results.csv"
        self.overrides_path = self.root / "vivino_overrides.csv"
        self.quarantine_path = self.root / "quarantine.csv"

    def tearDown(self) -> None:
        self.engine.dispose()
        self.tmpdir.cleanup()

    def _write_csv(self, path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _write_seed_files(self, override_description: str | None) -> None:
        self._write_seed_files_for_name(
            "2020 Test Producer - Test Cuvee - Red - 750 ml - Standard Bottle",
            override_description,
        )

    def _write_seed_files_for_name(self, wine_name: str, override_description: str | None) -> None:
        self._write_csv(
            self.comparison_path,
            [
                "name_plat",
                "year_plat",
                "quantity_plat",
                "volume_plat",
                "price_plat",
                "price_main",
                "price_diff",
                "price_diff_pct",
                "cheaper_side",
                "url_plat",
                "url_main",
                "platinum_in_stock",
                "grand_cru_in_stock",
                "image_url",
            ],
            [
                {
                    "name_plat": wine_name,
                    "year_plat": "2020",
                    "quantity_plat": "1",
                    "volume_plat": "750ml",
                    "price_plat": "120.00",
                    "price_main": "110.00",
                    "price_diff": "10.00",
                    "price_diff_pct": "9.09",
                    "cheaper_side": "Grand Cru Cheaper",
                    "url_plat": "https://example.com/platinum/test-cuvee",
                    "url_main": "https://example.com/grandcru/test-cuvee",
                    "platinum_in_stock": "true",
                    "grand_cru_in_stock": "false",
                    "image_url": "https://8362297.app.netsuite.com/core/media/media.nl?id=1",
                }
            ],
        )
        self._write_csv(
            self.vivino_path,
            [
                "wine_name",
                "vivino_rating",
                "vivino_num_ratings",
                "vivino_price",
                "vivino_url",
            ],
            [
                {
                    "wine_name": wine_name,
                    "vivino_rating": "4.2",
                    "vivino_num_ratings": "150",
                    "vivino_price": "135.00",
                    "vivino_url": "https://www.vivino.com/w/12345",
                }
            ],
        )
        override_rows: list[dict[str, str]] = []
        if override_description is not None:
            override_rows.append(
                {
                    "match_name": wine_name,
                    "wine_name": wine_name,
                    "vivino_rating": "4.2",
                    "vivino_num_ratings": "150",
                    "vivino_price": "135.00",
                    "vivino_description": override_description,
                    "vivino_url": "https://www.vivino.com/w/12345",
                    "notes": "test fixture",
                }
            )
        self._write_csv(
            self.overrides_path,
            [
                "match_name",
                "wine_name",
                "vivino_rating",
                "vivino_num_ratings",
                "vivino_price",
                "vivino_description",
                "vivino_url",
                "notes",
            ],
            override_rows,
        )

    def _run_import(self, *, quarantine_path: Path | None = None) -> None:
        with (
            patch.object(import_wine_data_module, "engine", self.engine),
            patch.object(import_wine_data_module, "SessionLocal", self.Session),
            patch.object(database_module, "engine", self.engine),
        ):
            import_data(
                self.comparison_path,
                self.vivino_path,
                self.overrides_path,
                quarantine_path=quarantine_path,
            )

    def _current_description(self) -> str | None:
        with self.Session() as session:
            deal = session.scalar(select(WineDeal))
            self.assertIsNotNone(deal)
            return deal.vivino_description

    def _current_deal(self) -> WineDeal:
        with self.Session() as session:
            deal = session.scalar(select(WineDeal))
            self.assertIsNotNone(deal)
            session.expunge(deal)
            return deal

    def _save_state(self, key: str, value) -> None:
        with patch.object(database_module, "engine", self.engine), patch.object(
            durable_state_module, "SessionLocal", self.Session
        ), patch.object(durable_state_module, "engine", self.engine):
            durable_state_module.save_state(key, value)

    def test_vivino_review_text_is_never_stored(self) -> None:
        self._write_seed_files("Bright cherry. Reminded me of my uncle's cellar, loved it!")
        self._run_import()
        self.assertIsNone(self._current_description())

    def test_csv_price_without_a_checked_date_is_not_shown(self) -> None:
        self._write_seed_files(None)
        self._run_import()
        deal = self._current_deal()
        self.assertIsNone(deal.vivino_price)
        self.assertIsNone(deal.vivino_checked_at)
        self.assertEqual(deal.vivino_rating, 4.2)

    def test_checked_vivino_facts_supply_price_rating_and_date(self) -> None:
        wine_name = "2020 Test Producer - Test Cuvee - Red - 750 ml - Standard Bottle"
        self._write_seed_files_for_name(wine_name, None)
        self._write_csv(
            self.vivino_path,
            ["wine_name", "vivino_rating", "vivino_num_ratings", "vivino_price", "vivino_url"],
            [{"wine_name": wine_name, "vivino_rating": "", "vivino_num_ratings": "", "vivino_price": "",
              "vivino_url": "https://www.vivino.com/US/en/test-cuvee/w/12345"}],
        )
        checked = datetime.now(UTC) - timedelta(days=2)
        self._save_state("vivino_facts", {
            "12345|2020": {"url": "https://www.vivino.com/en/test-cuvee/w/12345", "rating": 4.4,
                           "num_ratings": 321, "price": 60.0, "currency": "SGD",
                           "checked_at": checked.isoformat()},
        })
        self._run_import()
        deal = self._current_deal()
        self.assertEqual(deal.vivino_rating, 4.4)
        self.assertEqual(deal.vivino_num_ratings, 321)
        self.assertEqual(deal.vivino_price, 60.0)
        self.assertEqual(deal.vivino_url, "https://www.vivino.com/en/test-cuvee/w/12345?year=2020")
        self.assertEqual(deal.vivino_match_method, "exact")
        self.assertIsNotNone(deal.vivino_checked_at)

    def test_facts_older_than_a_month_are_not_shown(self) -> None:
        self._write_seed_files(None)
        old = datetime.now(UTC) - timedelta(days=45)
        self._save_state("vivino_facts", {
            "12345|2020": {"price": 99.0, "rating": 3.1, "num_ratings": 5, "checked_at": old.isoformat()},
        })
        self._run_import()
        deal = self._current_deal()
        self.assertIsNone(deal.vivino_price)
        self.assertEqual(deal.vivino_rating, 4.2)

    def test_resolver_overrides_in_database_survive_a_fresh_seed(self) -> None:
        wine_name = "2020 Fresh Estate - New Wine - Red - 750 ml - Standard Bottle"
        self._write_seed_files_for_name(wine_name, None)
        self._write_csv(
            self.vivino_path,
            ["wine_name", "vivino_rating", "vivino_num_ratings", "vivino_price", "vivino_url"],
            [],
        )
        self._save_state("vivino_auto_overrides", [{
            "match_name": wine_name, "wine_name": wine_name, "vivino_rating": "4.0",
            "vivino_num_ratings": "88", "vivino_url": "https://www.vivino.com/en/fresh-new-wine/w/777",
            "notes": "auto_resolved",
        }])
        self._run_import()
        deal = self._current_deal()
        self.assertEqual(deal.vivino_url, "https://www.vivino.com/en/fresh-new-wine/w/777")
        self.assertEqual(deal.vivino_rating, 4.0)

    def test_import_persists_availability_to_current_deal_and_snapshot(self) -> None:
        self._write_seed_files("Bright cherry, cedar, graphite.")
        self._run_import()

        with self.Session() as session:
            deal = session.scalar(select(WineDeal))
            snapshot = session.scalar(select(WineDealSnapshot))

        self.assertIsNotNone(deal)
        self.assertIsNotNone(snapshot)
        self.assertTrue(deal.platinum_in_stock)
        self.assertFalse(deal.grand_cru_in_stock)
        self.assertTrue(snapshot.platinum_in_stock)
        self.assertFalse(snapshot.grand_cru_in_stock)

    def _add_old_snapshot(self, days_ago: int) -> None:
        Base.metadata.create_all(bind=self.engine)
        with self.Session() as session:
            session.add(WineDealSnapshot(
                ingestion_run_id=0,
                captured_at=datetime.now(UTC) - timedelta(days=days_ago),
                wine_name="Old offer",
                price_platinum=100.0,
            ))
            session.commit()

    def _old_snapshot_count(self) -> int:
        with self.Session() as session:
            return len(session.scalars(select(WineDealSnapshot).where(WineDealSnapshot.wine_name == "Old offer")).all())

    def test_import_keeps_old_snapshots_when_retention_is_zero(self) -> None:
        self._write_seed_files("Bright cherry, cedar, graphite.")
        self._add_old_snapshot(400)
        with patch.object(import_wine_data_module, "settings", replace(import_wine_data_module.settings, history_retention_days=0)):
            self._run_import()
        self.assertEqual(self._old_snapshot_count(), 1)

    def test_import_prunes_old_snapshots_when_retention_is_set(self) -> None:
        self._write_seed_files("Bright cherry, cedar, graphite.")
        self._add_old_snapshot(400)
        with patch.object(import_wine_data_module, "settings", replace(import_wine_data_module.settings, history_retention_days=90)):
            self._run_import()
        self.assertEqual(self._old_snapshot_count(), 0)

    def test_import_persists_platinum_image_url(self) -> None:
        self._write_seed_files("Bright cherry, cedar, graphite.")
        self._run_import()

        deal = self._current_deal()

        self.assertEqual(deal.image_url, "https://8362297.app.netsuite.com/core/media/media.nl?id=1")

    def test_empty_comparison_refuses_to_replace_existing_data(self) -> None:
        self._write_seed_files("Keep this row.")
        self._run_import()

        self._write_csv(
            self.comparison_path,
            [
                "name_plat",
                "year_plat",
                "quantity_plat",
                "volume_plat",
                "price_plat",
                "price_main",
                "price_diff",
                "price_diff_pct",
                "cheaper_side",
                "url_plat",
                "url_main",
            ],
            [],
        )

        with self.assertRaisesRegex(RuntimeError, "zero rows"):
            self._run_import()

        self.assertEqual(self._current_deal().price_platinum, 120.0)

    def test_url_only_match_is_tracked_separately_from_exact_match(self) -> None:
        wine_name = "2020 Test Producer - Url Only Cuvee - Red - 750 ml - Standard Bottle"
        self._write_seed_files_for_name(wine_name, None)
        self._write_csv(
            self.vivino_path,
            [
                "wine_name",
                "vivino_rating",
                "vivino_num_ratings",
                "vivino_price",
                "vivino_url",
            ],
            [
                {
                    "wine_name": wine_name,
                    "vivino_rating": "",
                    "vivino_num_ratings": "",
                    "vivino_price": "",
                    "vivino_url": "https://www.vivino.com/en/example/w/12345",
                }
            ],
        )

        self._run_import()

        deal = self._current_deal()
        self.assertEqual(deal.vivino_match_method, "url_only")
        self.assertEqual(deal.vivino_url, "https://www.vivino.com/en/example/w/12345")
        self.assertIsNone(deal.vivino_rating)
        self.assertIsNone(deal.vivino_num_ratings)

    def test_offer_without_vivino_match_is_shown_unscored(self) -> None:
        good_name = "2022 Lapis Luna - Cabernet Sauvignon - Red - 750 ml - Standard Bottle"
        bad_name = "2020 Unknown Estate - Missing Metadata - Red - 750 ml - Standard Bottle"
        comparison_fields = [
            "name_plat",
            "year_plat",
            "quantity_plat",
            "volume_plat",
            "price_plat",
            "price_main",
            "price_diff",
            "price_diff_pct",
            "cheaper_side",
            "url_plat",
            "url_main",
        ]
        self._write_csv(
            self.comparison_path,
            comparison_fields,
            [
                {
                    "name_plat": good_name,
                    "year_plat": "2022",
                    "quantity_plat": "1",
                    "volume_plat": "750ml",
                    "price_plat": "120.00",
                    "price_main": "110.00",
                    "price_diff": "10.00",
                    "price_diff_pct": "9.09",
                    "cheaper_side": "Grand Cru Cheaper",
                    "url_plat": "https://example.com/platinum/good",
                    "url_main": "https://example.com/grandcru/good",
                },
                {
                    "name_plat": bad_name,
                    "year_plat": "2020",
                    "quantity_plat": "1",
                    "volume_plat": "750ml",
                    "price_plat": "90.00",
                    "price_main": "95.00",
                    "price_diff": "-5.00",
                    "price_diff_pct": "-5.26",
                    "cheaper_side": "Platinum Cheaper",
                    "url_plat": "https://example.com/platinum/bad",
                    "url_main": "https://example.com/grandcru/bad",
                },
            ],
        )
        self._write_csv(
            self.vivino_path,
            [
                "wine_name",
                "vivino_rating",
                "vivino_num_ratings",
                "vivino_price",
                "vivino_url",
            ],
            [
                {
                    "wine_name": good_name,
                    "vivino_rating": "4.0",
                    "vivino_num_ratings": "7963",
                    "vivino_price": "50.00",
                    "vivino_url": "https://www.vivino.com/SG/en/lapis-luna-cabernet-sauvignon-lodi/w/6612844",
                }
            ],
        )
        self._write_csv(
            self.overrides_path,
            [
                "match_name",
                "wine_name",
                "vivino_rating",
                "vivino_num_ratings",
                "vivino_price",
                "vivino_description",
                "vivino_url",
                "notes",
            ],
            [],
        )

        self._run_import(quarantine_path=self.quarantine_path)

        with self.Session() as session:
            deals = {deal.wine_name: deal for deal in session.scalars(select(WineDeal)).all()}
        self.assertEqual(set(deals), {good_name, bad_name})
        self.assertIsNotNone(deals[good_name].deal_score)
        unmatched = deals[bad_name]
        self.assertEqual(unmatched.price_platinum, 90.0)
        self.assertIsNone(unmatched.vivino_price)
        self.assertIsNone(unmatched.deal_score)

        quarantine_rows = list(csv.DictReader(self.quarantine_path.open(encoding="utf-8")))
        self.assertEqual(quarantine_rows, [])

        with patch.object(validate_module, "SessionLocal", self.Session):
            report = validate_module.run_validation()
        self.assertEqual(report["errors"], [])
        self.assertTrue(any("missing country" in warning for warning in report["warnings"]))

    def test_platinum_listing_fields_fill_unmatched_offer(self) -> None:
        name = "2023 Tenuta Perano - Chianti Classico - Red - 750 ml - Standard Bottle"
        fields = [
            "name_plat", "year_plat", "quantity_plat", "volume_plat", "price_plat", "cheaper_side",
            "url_plat", "platinum_vivino_rating", "platinum_country", "platinum_region", "platinum_grapes",
        ]
        self._write_csv(
            self.comparison_path,
            fields,
            [{
                "name_plat": name, "year_plat": "2023", "quantity_plat": "1", "volume_plat": "750ml",
                "price_plat": "48.00", "cheaper_side": "No Match", "url_plat": "https://example.com/p",
                "platinum_vivino_rating": "3.9", "platinum_country": "Italy",
                "platinum_region": "Tuscany", "platinum_grapes": "Sangiovese",
            }],
        )
        self._write_csv(self.vivino_path, ["wine_name", "vivino_rating", "vivino_num_ratings", "vivino_price", "vivino_url"], [])
        self._write_csv(self.overrides_path, ["match_name", "wine_name", "vivino_url"], [])

        self._run_import(quarantine_path=self.quarantine_path)

        deal = self._current_deal()
        self.assertEqual(deal.vivino_match_method, "platinum")
        self.assertEqual(deal.vivino_rating, 3.9)
        self.assertIsNone(deal.vivino_price)
        self.assertIsNotNone(deal.deal_score)
        self.assertEqual((deal.country, deal.region, deal.grapes), ("Italy", "Tuscany", "Sangiovese"))

    def test_quarantine_only_hides_rows_without_platinum_price(self) -> None:
        self.assertEqual(import_wine_data_module._quarantine_reasons({"price_platinum": None}), ["missing_price_platinum"])
        self.assertEqual(import_wine_data_module._quarantine_reasons({"price_platinum": 90.0, "country": None}), [])


if __name__ == "__main__":
    unittest.main()
