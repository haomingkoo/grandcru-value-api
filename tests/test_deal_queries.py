import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.models import WineDeal
from app.service import list_deals


class DealQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, expire_on_commit=False)
        self.session = self.Session()

        self.session.add_all(
            [
                WineDeal(
                    wine_name="Value Pick One",
                    platinum_url="https://example.com/value-one",
                    price_platinum=80.0,
                    price_grand_cru=100.0,
                    price_diff=-20.0,
                    price_diff_pct=-20.0,
                    cheaper_side="Platinum Cheaper",
                    vivino_rating=4.2,
                    vivino_num_ratings=150,
                    deal_score=45.0,
                    country="France",
                    region="Burgundy",
                    wine_type="Red",
                    style_family="Red",
                    offering_type="Single Bottle",
                    producer="Example Producer",
                    volume="750ml",
                    quantity=1,
                ),
                WineDeal(
                    wine_name="Champagne Markup Bottle",
                    platinum_url="https://example.com/markup",
                    price_platinum=130.0,
                    price_grand_cru=100.0,
                    price_diff=30.0,
                    price_diff_pct=30.0,
                    cheaper_side="Grand Cru Cheaper",
                    vivino_rating=4.1,
                    vivino_num_ratings=210,
                    deal_score=33.0,
                    country="France",
                    region="Champagne",
                    wine_type="Sparkling",
                    style_family="Champagne",
                    offering_type="Single Bottle",
                    producer="Markup Estate",
                    volume="750ml",
                    quantity=1,
                ),
                WineDeal(
                    wine_name="No Match Discovery",
                    platinum_url="https://example.com/no-match",
                    price_platinum=60.0,
                    price_grand_cru=None,
                    price_diff=None,
                    price_diff_pct=None,
                    cheaper_side="No Match",
                    vivino_rating=4.0,
                    vivino_num_ratings=80,
                    deal_score=28.0,
                    country="Italy",
                    region="Piedmont",
                    wine_type="White",
                    style_family="Sweet / Dessert",
                    offering_type="Single Bottle",
                    producer="Discovery House",
                    volume="750ml",
                    quantity=1,
                ),
            ]
        )
        self.session.commit()

    def tearDown(self) -> None:
        self.session.close()
        self.engine.dispose()

    def test_comparable_only_filter_excludes_no_match_rows(self) -> None:
        deals = list_deals(self.session, comparable_only=True, sort_by="wine_name", sort_order="asc")

        self.assertEqual([deal.wine_name for deal in deals], ["Champagne Markup Bottle", "Value Pick One"])

    def test_absolute_gap_sort_orders_by_largest_gap(self) -> None:
        deals = list_deals(self.session, comparable_only=True, sort_by="price_diff_pct_abs", sort_order="desc")

        self.assertEqual(deals[0].wine_name, "Champagne Markup Bottle")
        self.assertEqual(deals[1].wine_name, "Value Pick One")
        self.assertEqual(deals[0].value_verdict, "Platinum Markup")
        self.assertEqual(deals[1].value_verdict, "Strong Credit Spend")

    def test_style_family_filter_targets_browse_category(self) -> None:
        deals = list_deals(self.session, style_family="Champagne", sort_by="wine_name", sort_order="asc")

        self.assertEqual([deal.wine_name for deal in deals], ["Champagne Markup Bottle"])


if __name__ == "__main__":
    unittest.main()
