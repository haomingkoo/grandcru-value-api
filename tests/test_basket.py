import unittest
from types import SimpleNamespace

from app.basket import candidate_items, suggest


def deal(i, price, rating=4.0, style="Red", qty=1, volume="750ml", in_stock=True, gc=None, count=100):
    return SimpleNamespace(id=i, wine_name=f"Wine {i}", price_platinum=price, vivino_rating=rating, vivino_num_ratings=count,
                           style_family=style, wine_type=style, quantity=qty, volume=volume, volume_ml=None,
                           platinum_in_stock=in_stock, price_grand_cru=gc)


class BasketTests(unittest.TestCase):
    def test_reaches_threshold_with_least_overspend(self):
        deals = [deal(1, 120), deal(2, 95), deal(3, 85), deal(4, 60), deal(5, 150)]
        out = suggest(candidate_items(deals, styles=None, include_unrated=False), target=300, strict=False)
        self.assertEqual(out[0]["kind"], "closest")
        self.assertEqual(out[0]["total"], 300.0)  # 120 + 95 + 85
        self.assertEqual(out[0]["credit_back"], 200.0)
        self.assertFalse(out[0]["free_delivery"])
        self.assertEqual(out[0]["net_after_credit"], 130.0)  # 300 + 30 delivery - 200 back

    def test_free_delivery_needs_strictly_over_350(self):
        deals = [deal(1, 200), deal(2, 150), deal(3, 151)]
        out = suggest(candidate_items(deals, styles=None, include_unrated=False), target=350, strict=True)
        self.assertEqual(out[0]["total"], 351.0)
        self.assertTrue(out[0]["free_delivery"])

    def test_sold_out_unrated_and_other_styles_are_left_out(self):
        deals = [deal(1, 100), deal(2, 100, in_stock=False), deal(3, 100, rating=None), deal(4, 100, style="White")]
        items = candidate_items(deals, styles={"Red"}, include_unrated=False)
        self.assertEqual([i.deal_id for i in items], [1])
        with_unrated = candidate_items(deals, styles={"Red"}, include_unrated=True)
        self.assertEqual(sorted(i.deal_id for i in with_unrated), [1, 3])

    def test_max_bottles_counts_bundles(self):
        deals = [deal(1, 300, qty=3), deal(2, 100), deal(3, 100), deal(4, 100)]
        out = suggest(candidate_items(deals, styles=None, include_unrated=False), target=300, strict=False, max_bottles=3)
        self.assertTrue(all(b["bottles"] <= 3 for b in out))

    def test_no_basket_when_the_threshold_is_out_of_reach(self):
        out = suggest(candidate_items([deal(1, 50)], styles=None, include_unrated=False), target=300, strict=False)
        self.assertEqual(out, [])

    def test_grand_cru_difference_only_counts_known_prices(self):
        deals = [deal(1, 150, gc=170), deal(2, 150)]
        out = suggest(candidate_items(deals, styles=None, include_unrated=False), target=300, strict=False)
        self.assertEqual((out[0]["grand_cru_difference"], out[0]["grand_cru_known"]), (20.0, 1))

    def test_half_bottles_count_as_half(self):
        items = candidate_items([deal(1, 65, volume="375ml")], styles=None, include_unrated=False)
        self.assertEqual((items[0].per_750, items[0].bottles_750), (130.0, 0.5))


if __name__ == "__main__":
    unittest.main()
