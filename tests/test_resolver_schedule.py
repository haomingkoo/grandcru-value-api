import unittest
from datetime import UTC, datetime, timedelta

from scripts.resolve_vivino_matches import due_for_search, search_priority
from scripts.vivino_fetch import identity_drift, is_fact_fresh

DAY = 86400


class ScheduleTests(unittest.TestCase):
    def test_unmatched_wines_back_off_1_3_7_30_days(self):
        now = 100 * DAY
        state = {"seen_unresolved": {"w": now - 2 * DAY}, "search_attempts": {"w": 1}}
        self.assertTrue(due_for_search("w", state, now))
        state["search_attempts"]["w"] = 2
        self.assertFalse(due_for_search("w", state, now))
        state["seen_unresolved"]["w"] = now - 8 * DAY
        state["search_attempts"]["w"] = 3
        self.assertTrue(due_for_search("w", state, now))
        state["search_attempts"]["w"] = 9
        self.assertFalse(due_for_search("w", state, now))
        self.assertTrue(due_for_search("new", state, now))

    def test_in_stock_and_dearer_wines_are_searched_first(self):
        rows = [
            {"name_plat": "a", "price_plat": "50", "platinum_in_stock": "true"},
            {"name_plat": "b", "price_plat": "900", "platinum_in_stock": "false"},
            {"name_plat": "c", "price_plat": "300", "platinum_in_stock": "true"},
        ]
        self.assertEqual([r["name_plat"] for r in sorted(rows, key=search_priority)], ["c", "a", "b"])

    def test_wines_without_an_sgd_price_are_rechecked_monthly(self):
        now = datetime(2026, 10, 26, tzinfo=UTC)
        checked = (now - timedelta(days=10)).isoformat()
        self.assertFalse(is_fact_fresh({"checked_at": checked, "price": 55.0}, now))
        self.assertTrue(is_fact_fresh({"checked_at": checked, "price": None}, now))


class DriftTests(unittest.TestCase):
    def test_vintage_and_producer_checks(self):
        offer = "2021 Miraval - Cotes de Provence Rose - Rose - 750 ml - Standard Bottle"
        self.assertIsNone(identity_drift(offer, {"name": "Miraval Côtes de Provence Rosé 2021"}))
        self.assertIn("vintage", identity_drift(offer, {"name": "Miraval Côtes de Provence Rosé 2019"}))
        self.assertIn("producer", identity_drift(offer, {"name": "Whispering Angel Rosé 2021"}))
        self.assertIsNone(identity_drift(offer, {"name": ""}))
        self.assertIsNone(identity_drift("NV Felix Solis - Mucho Mas Gold - Red", {"name": "Félix Solís Mucho Más Gold"}))
        # Estate naming on Vivino: no producer word, but the label matches.
        self.assertIsNone(identity_drift("2024 Frescobaldi - Rose Alie Ammiraglia - Rose - 750 ml", {"name": "2024 Tenuta Ammiraglia Alìe Rosé Toscana"}))
        # A different producer that shares one name.
        self.assertIn("part of producer", identity_drift("2021 Hudelot - Baillet - Bonnes Mares - Red - 750 ml", {"name": "2021 Daniel Moine-Hudelot Bonnes Mares Grand Cru"}))
        self.assertIsNone(identity_drift("2023 Hudelot - Baillet - Chambolle Musigny les Cras - Red", {"name": "2023 Hudelot-Baillet Chambolle-Musigny 1er Cru 'Les Cras'"}))


if __name__ == "__main__":
    unittest.main()
