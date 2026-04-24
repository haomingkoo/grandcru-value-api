import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import app.database as database_module
import scripts.import_wine_data as import_wine_data_module
from app.database import Base
from app.models import WineDeal
from scripts.import_wine_data import (
    _resolve_vivino_price_to_listing,
    _scale_vivino_price_to_listing,
    default_skip_if_fresh_hours,
    import_data,
)


class ImportWineDataTests(unittest.TestCase):
    def test_default_skip_if_fresh_protects_railway_web_startup(self) -> None:
        with patch.dict("os.environ", {"RAILWAY_SERVICE_NAME": "web"}, clear=False):
            self.assertEqual(default_skip_if_fresh_hours(), 20.0)

    def test_default_skip_if_fresh_keeps_manual_imports_immediate(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(default_skip_if_fresh_hours(), 0.0)

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

    def _run_import(self) -> None:
        with (
            patch.object(import_wine_data_module, "engine", self.engine),
            patch.object(import_wine_data_module, "SessionLocal", self.Session),
            patch.object(database_module, "engine", self.engine),
        ):
            import_data(
                self.comparison_path,
                self.vivino_path,
                self.overrides_path,
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

    def test_import_preserves_existing_description_when_csv_loses_it(self) -> None:
        self._write_seed_files("Bright cherry, cedar, graphite.")
        self._run_import()
        self.assertEqual(self._current_description(), "Bright cherry, cedar, graphite.")

        self._write_seed_files(None)
        self._run_import()

        self.assertEqual(self._current_description(), "Bright cherry, cedar, graphite.")

    def test_csv_description_overrides_preserved_database_value(self) -> None:
        self._write_seed_files("Older cellar note.")
        self._run_import()
        self.assertEqual(self._current_description(), "Older cellar note.")

        self._write_seed_files("Fresh override note.")
        self._run_import()

        self.assertEqual(self._current_description(), "Fresh override note.")

    def test_import_preserves_description_by_vivino_url_when_name_changes(self) -> None:
        self._write_seed_files("Mineral citrus, chalk, saline finish.")
        self._run_import()
        self.assertEqual(self._current_description(), "Mineral citrus, chalk, saline finish.")

        self._write_seed_files_for_name(
            "2020 Test Producer Test Cuvee Rouge 750ml",
            None,
        )
        self._run_import()

        self.assertEqual(self._current_description(), "Mineral citrus, chalk, saline finish.")

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


if __name__ == "__main__":
    unittest.main()
