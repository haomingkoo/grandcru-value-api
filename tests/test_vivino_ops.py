import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.models import WineDeal
from app.service import list_vivino_unresolved_export_rows
from scripts.vivino_overrides import is_locked_override_row, upsert_overrides


class VivinoOverrideTests(unittest.TestCase):
    def test_manual_note_rows_are_treated_as_locked(self) -> None:
        row = {"match_name": "Locked Wine", "notes": "manual verified correct page"}
        self.assertTrue(is_locked_override_row(row))

    def test_upsert_does_not_overwrite_locked_rows(self) -> None:
        existing = [
            {
                "match_name": "Locked Wine",
                "wine_name": "Locked Wine",
                "vivino_rating": "4.3",
                "vivino_num_ratings": "99",
                "vivino_price": "120.0",
                "vivino_description": "manual review kept this row",
                "vivino_url": "https://example.com/manual",
                "locked": "",
                "notes": "manual verified correct page",
            }
        ]
        incoming = [
            {
                "match_name": "Locked Wine",
                "wine_name": "Wrong Auto Match",
                "vivino_rating": "3.5",
                "vivino_num_ratings": "12",
                "vivino_price": "20.0",
                "vivino_description": "bad overwrite",
                "vivino_url": "https://example.com/bad",
                "locked": "",
                "notes": "cached resolved_at=1",
            }
        ]

        merged = upsert_overrides(existing, incoming)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["wine_name"], "Locked Wine")
        self.assertEqual(merged[0]["vivino_url"], "https://example.com/manual")
        self.assertEqual(merged[0]["locked"], "1")

    def test_upsert_updates_unlocked_rows(self) -> None:
        existing = [
            {
                "match_name": "Open Wine",
                "wine_name": "Open Wine",
                "vivino_rating": "",
                "vivino_num_ratings": "",
                "vivino_price": "",
                "vivino_description": "",
                "vivino_url": "",
                "locked": "",
                "notes": "",
            }
        ]
        incoming = [
            {
                "match_name": "Open Wine",
                "wine_name": "Open Wine",
                "vivino_rating": "4.1",
                "vivino_num_ratings": "555",
                "vivino_price": "88.0",
                "vivino_description": "new automated match",
                "vivino_url": "https://example.com/good",
                "locked": "",
                "notes": "cached resolved_at=2",
            }
        ]

        merged = upsert_overrides(existing, incoming)

        self.assertEqual(merged[0]["vivino_rating"], "4.1")
        self.assertEqual(merged[0]["vivino_url"], "https://example.com/good")
        self.assertEqual(merged[0]["locked"], "")


class VivinoUnresolvedExportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, expire_on_commit=False)
        self.session = self.Session()
        self.session.add_all(
            [
                WineDeal(
                    wine_name="Unlocked Unrated",
                    platinum_url="https://example.com/unlocked",
                    price_platinum=45.0,
                    price_grand_cru=45.0,
                    price_diff=0.0,
                    price_diff_pct=0.0,
                    cheaper_side="Same Price",
                    quantity=1,
                    volume="750ml",
                    deal_score=0.0,
                ),
                WineDeal(
                    wine_name="Locked Unrated",
                    platinum_url="https://example.com/locked",
                    price_platinum=55.0,
                    price_grand_cru=55.0,
                    price_diff=0.0,
                    price_diff_pct=0.0,
                    cheaper_side="Same Price",
                    quantity=1,
                    volume="750ml",
                    deal_score=0.0,
                ),
                WineDeal(
                    wine_name="Already Rated",
                    platinum_url="https://example.com/rated",
                    price_platinum=60.0,
                    price_grand_cru=50.0,
                    price_diff=10.0,
                    price_diff_pct=20.0,
                    cheaper_side="Grand Cru Cheaper",
                    quantity=1,
                    volume="750ml",
                    vivino_rating=4.2,
                    vivino_num_ratings=321,
                    deal_score=25.0,
                ),
            ]
        )
        self.session.commit()

    def tearDown(self) -> None:
        self.session.close()
        self.engine.dispose()

    def test_export_excludes_locked_and_rated_rows_by_default(self) -> None:
        rows = list_vivino_unresolved_export_rows(
            self.session,
            locked_wine_names={"Locked Unrated"},
        )

        self.assertEqual([row["name_plat"] for row in rows], ["Unlocked Unrated"])
        self.assertEqual(rows[0]["price_plat"], "45.00")
        self.assertEqual(rows[0]["cheaper_side"], "Same Price")

    def test_export_can_include_locked_rows(self) -> None:
        rows = list_vivino_unresolved_export_rows(
            self.session,
            include_locked=True,
            locked_wine_names={"Locked Unrated"},
        )

        self.assertEqual(
            [row["name_plat"] for row in rows],
            ["Locked Unrated", "Unlocked Unrated"],
        )


if __name__ == "__main__":
    unittest.main()
