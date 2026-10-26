import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.models import WineDeal
from app.service import fill_volume_ml, list_deals


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


if __name__ == "__main__":
    unittest.main()
