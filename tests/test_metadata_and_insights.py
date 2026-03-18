import unittest
from types import SimpleNamespace

from app.deal_insights import classify_price_trend, compute_deal_insights
from app.wine_metadata import derive_wine_metadata


class MetadataAndInsightsTests(unittest.TestCase):
    def test_derive_wine_metadata_captures_sources_and_confidence(self) -> None:
        metadata = derive_wine_metadata(
            wine_name="2019 Nervi - Gattinara Vigneto Molsino - Red - 750 ml - Standard Bottle",
            quantity=1,
            volume="750ml",
        )

        self.assertEqual(metadata.country, "Italy")
        self.assertEqual(metadata.region, "Piedmont")
        self.assertEqual(metadata.grapes, "Nebbiolo")
        self.assertEqual(metadata.style_family, "Red")
        self.assertEqual(metadata.origin_source, "listing_keyword_heuristic")
        self.assertEqual(metadata.origin_confidence, "medium")
        self.assertEqual(metadata.grape_source, "regional_inference")
        self.assertEqual(metadata.grape_confidence, "medium")
        self.assertEqual(metadata.metadata_confidence, "medium")

    def test_derive_wine_metadata_detects_sweet_dessert_style(self) -> None:
        metadata = derive_wine_metadata(
            wine_name="2023 Botter - Moscato d'Asti DOCG - White - 750 ml - Standard Bottle",
            quantity=1,
            volume="750ml",
        )

        self.assertEqual(metadata.style_family, "Sweet / Dessert")
        self.assertEqual(metadata.grapes, "Moscato Bianco")

    def test_compute_deal_insights_marks_strong_credit_spend(self) -> None:
        deal = SimpleNamespace(
            wine_name="2022 Domaine Claude Dugat - Gevrey Chambertin - Red - 750 ml - Standard Bottle (Bundle of 3)",
            quantity=3,
            volume="750ml",
            price_diff_pct=-12.5,
            cheaper_side="Platinum Cheaper",
            vivino_rating=4.3,
            vivino_num_ratings=5288,
            price_platinum_change_7d=-10.0,
            price_grand_cru_change_7d=0.0,
            price_platinum_change_30d=None,
            price_grand_cru_change_30d=None,
        )

        insights = compute_deal_insights(deal)

        self.assertTrue(insights.has_competitor_match)
        self.assertTrue(insights.is_platinum_cheaper)
        self.assertTrue(insights.is_good_wine)
        self.assertTrue(insights.is_high_confidence)
        self.assertEqual(insights.value_verdict, "Strong Credit Spend")
        self.assertEqual(insights.value_verdict_tone, "good")
        self.assertEqual(insights.platinum_trend_7d, "down")
        self.assertEqual(insights.grand_cru_trend_7d, "flat")
        self.assertEqual(insights.platinum_trend_30d, "unknown")

    def test_classify_price_trend(self) -> None:
        self.assertEqual(classify_price_trend(-1.0), "down")
        self.assertEqual(classify_price_trend(1.0), "up")
        self.assertEqual(classify_price_trend(0.0), "flat")
        self.assertEqual(classify_price_trend(None), "unknown")


if __name__ == "__main__":
    unittest.main()
