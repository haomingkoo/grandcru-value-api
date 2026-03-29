import unittest

from scripts.import_wine_data import (
    _resolve_vivino_price_to_listing,
    _scale_vivino_price_to_listing,
)


class ImportWineDataTests(unittest.TestCase):
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
        )

        self.assertIsNone(adjusted)


if __name__ == "__main__":
    unittest.main()
