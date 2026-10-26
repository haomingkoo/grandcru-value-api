import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.models import WineDeal
from datetime import UTC, date, datetime

from app.models import PriceObservation
from app.service import fill_volume_ml, get_deal_stats, list_deals


class PricePer750Tests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()
        self.session.add_all([
            WineDeal(wine_name="Half bottle", volume="375ml", quantity=1, price_platinum=65.0),     # 130
            WineDeal(wine_name="Magnum", volume="1.5l", quantity=1, price_platinum=390.0),          # 195
            WineDeal(wine_name="Bundle of 3", volume="750ml", quantity=3, price_platinum=300.0),    # 100
            WineDeal(wine_name="Standard", volume="750ml", quantity=1, price_platinum=157.0),       # 157
            WineDeal(wine_name="No size", volume=None, quantity=1, price_platinum=50.0),
        ])
        self.session.commit()
        fill_volume_ml(self.session)

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def test_sort_by_price_per_750_puts_unknown_sizes_last(self):
        deals = list_deals(self.session, sort_by="price_per_750", sort_order="asc")
        self.assertEqual([d.wine_name for d in deals], ["Bundle of 3", "Half bottle", "Standard", "Magnum", "No size"])
        self.assertEqual([d.price_platinum_per_750 for d in deals[:4]], [100.0, 130.0, 157.0, 195.0])

    def test_filter_by_price_per_750_range(self):
        deals = list_deals(self.session, min_price_per_750=120, max_price_per_750=160)
        self.assertEqual(sorted(d.wine_name for d in deals), ["Half bottle", "Standard"])


class AvailabilityTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite://")
        Base.metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)()
        self.session.add_all([
            WineDeal(wine_name="In stock", volume="750ml", quantity=1, price_platinum=50.0, platinum_in_stock=True),
            WineDeal(wine_name="Went out", volume="750ml", quantity=1, price_platinum=60.0, platinum_in_stock=False),
            WineDeal(wine_name="Never seen in stock", volume="750ml", quantity=1, price_platinum=70.0, platinum_in_stock=False),
        ])

        def obs(name, day, in_stock):
            return PriceObservation(wine_name=name, source="platinum", price=60.0, checked_at=datetime(2026, 10, day, tzinfo=UTC),
                                    observed_on=date(2026, 10, day), in_stock=in_stock, origin="daily_check")
        self.session.add_all([
            obs("Went out", 20, None), obs("Went out", 21, True), obs("Went out", 22, False), obs("Went out", 23, False),
            obs("Never seen in stock", 23, False),
        ])
        self.session.commit()

    def tearDown(self):
        self.session.close()
        self.engine.dispose()

    def test_availability_filter_and_counts(self):
        self.assertEqual([d.wine_name for d in list_deals(self.session, availability="in_stock")], ["In stock"])
        self.assertEqual(len(list_deals(self.session, availability="sold_out")), 2)
        stats = get_deal_stats(self.session, availability="in_stock")
        self.assertEqual((stats["total_deals"], stats["in_stock_count"], stats["sold_out_count"]), (1, 1, 2))

    def test_sold_out_since_only_after_being_seen_in_stock(self):
        by_name = {d.wine_name: d for d in list_deals(self.session, availability="sold_out")}
        self.assertEqual(by_name["Went out"].sold_out_since, date(2026, 10, 22))
        self.assertIsNone(by_name["Never seen in stock"].sold_out_since)
        self.assertEqual(by_name["Never seen in stock"].first_seen_on, date(2026, 10, 23))


if __name__ == "__main__":
    unittest.main()
