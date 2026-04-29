from scripts.build_comparison_summary import build_matches, build_summary, package_type, prepare_rows


def test_package_type_detects_gift_sets() -> None:
    assert package_type("NV Charles Heidsieck Brut Reserve Gift Box - 2 glasses") == "gift_set"
    assert package_type("NV Charles Heidsieck Brut Reserve Standard Bottle") == "standard"


def test_prepare_rows_can_filter_out_of_stock_grandcru_rows() -> None:
    rows = prepare_rows(
        [
            {
                "name": "NV Charles Heidsieck - Brut Reserve",
                "price": "125.00",
                "url": "https://grandcruwines.com/products/nv-charles-heidsieck-brut-reserve",
                "in_stock": "false",
            },
            {
                "name": "NV Charles Heidsieck - Blanc de Blancs",
                "price": "161.00",
                "url": "https://grandcruwines.com/products/nv-charles-heidsieck-blanc-de-blancs",
                "in_stock": "true",
            },
        ],
        enforce_in_stock=True,
    )

    assert [row["name"] for row in rows] == ["NV Charles Heidsieck - Blanc de Blancs"]


def test_standard_bottle_does_not_match_gift_set_variant() -> None:
    grandcru = prepare_rows(
        [
            {
                "name": "NV Charles Heidsieck - Brut Reserve (Gift Box - 2 glasses)",
                "price": "120.00",
                "url": "https://grandcruwines.com/products/nv-charles-heidsieck-brut-reserve-gift-box-2-glasses",
                "in_stock": "true",
            }
        ],
        enforce_in_stock=True,
    )
    platinum = prepare_rows(
        [
            {
                "name": "NV Charles Heidsieck - Brut Reserve - White - 750 ml - Standard Bottle",
                "price": "100.00",
                "url": "https://platwineclub.wineportal.com/wines/nv-charles-heidsieck-brut-reserve-white-750-ml-standard-bottle",
                "in_stock": "true",
            }
        ],
        enforce_in_stock=True,
    )

    matched = build_matches(grandcru, platinum, threshold=0.6)
    summary = build_summary(matched)

    assert summary[0]["cheaper_side"] == "No Match"
    assert summary[0]["price_main"] is None
    assert summary[0]["url_main"] == ""


def test_brut_reserve_does_not_match_rose_reserve() -> None:
    grandcru = prepare_rows(
        [
            {
                "name": "NV Charles Heidsieck - Rose Reserve",
                "price": "136.00",
                "url": "https://grandcruwines.com/products/nv-charles-heidsieck-rose-reserve",
                "in_stock": "true",
            }
        ],
        enforce_in_stock=True,
    )
    platinum = prepare_rows(
        [
            {
                "name": "NV Charles Heidsieck - Brut Reserve - White - 750 ml - Standard Bottle",
                "price": "100.00",
                "url": "https://platwineclub.wineportal.com/wines/nv-charles-heidsieck-brut-reserve-white-750-ml-standard-bottle",
                "in_stock": "true",
            }
        ],
        enforce_in_stock=True,
    )

    matched = build_matches(grandcru, platinum, threshold=0.6)
    summary = build_summary(matched)

    assert summary[0]["cheaper_side"] == "No Match"
    assert summary[0]["url_main"] == ""
