import time

from scripts.llm_grandcru_resolver import _candidate_pool, _apply_match, comparison_fieldnames, resolve_rows
from scripts.llm_utils import cache_key
from scripts.build_comparison_summary import prepare_rows


def test_candidate_pool_prefers_same_year_volume_rows() -> None:
    platinum_row = {
        "name_plat": "2024 Famille Perrin - Cotes du Rhone Reserve Blanc - White - 750 ml - Standard Bottle",
        "price_plat": "45.00",
        "url_plat": "https://platwineclub.wineportal.com/wines/2024-famille-perrin-cotes-du-rhone-reserve-blanc-white-750-ml-standard-bottle",
    }
    grandcru_rows = prepare_rows(
        [
            {
                "name": "2024 Famille Perrin - Cotes du Rhone Reserve Blanc",
                "price": "45.00",
                "url": "https://grandcruwines.com/products/2024-famille-perrin-cotes-du-rhone-reserve-blanc",
            },
            {
                "name": "2023 Famille Perrin - Cotes du Rhone Reserve Blanc",
                "price": "44.00",
                "url": "https://grandcruwines.com/products/2023-famille-perrin-cotes-du-rhone-reserve-blanc",
            },
        ],
        enforce_in_stock=False,
    )

    candidates = _candidate_pool(platinum_row, grandcru_rows, max_candidates=8)

    assert candidates
    assert candidates[0].url == "https://grandcruwines.com/products/2024-famille-perrin-cotes-du-rhone-reserve-blanc"


def test_candidate_pool_can_use_catalog_rows_when_grandcru_is_out_of_stock() -> None:
    platinum_row = {
        "name_plat": "2022 La Croix de Brully - Chassagne-Montrachet La Goujonne - White - 750 ml - Standard Bottle",
        "price_plat": "130.00",
        "url_plat": "https://platwineclub.wineportal.com/wines/2022-la-croix-de-brully-chassagne-montrachet-la-goujonne-white-750-ml-standard-bottle",
    }
    grandcru_rows = prepare_rows(
        [
            {
                "name": "2022 La Croix de Brully - Chassagne-Montrachet La Goujonne",
                "price": "130.00",
                "url": "https://grandcruwines.com/products/2022-la-croix-de-brully-chassagne-montrachet-la-goujonne",
                "in_stock": "false",
            }
        ],
        enforce_in_stock=False,
    )

    candidates = _candidate_pool(platinum_row, grandcru_rows, max_candidates=8)

    assert candidates
    assert candidates[0].url == "https://grandcruwines.com/products/2022-la-croix-de-brully-chassagne-montrachet-la-goujonne"


def test_resolve_rows_applies_cached_match() -> None:
    comparison_rows = [
        {
            "name_plat": "2024 Famille Perrin - Cotes du Rhone Reserve Blanc - White - 750 ml - Standard Bottle",
            "year_plat": "2024",
            "quantity_plat": "1",
            "volume_plat": "750ml",
            "quantity_main": "",
            "price_plat": "45.00",
            "price_main": "",
            "price_diff": "",
            "price_diff_pct": "",
            "cheaper_side": "No Match",
            "url_plat": "https://platwineclub.wineportal.com/wines/2024-famille-perrin-cotes-du-rhone-reserve-blanc-white-750-ml-standard-bottle",
            "url_main": "",
            "platinum_in_stock": "true",
            "grand_cru_in_stock": "",
            "image_url": "https://8362297.app.netsuite.com/core/media/media.nl?id=1",
            "platinum_vivino_rating": "",
            "platinum_vivino_num_ratings": "",
            "platinum_vivino_url": "",
        }
    ]
    grandcru_rows = prepare_rows(
        [
            {
                "name": "2024 Famille Perrin - Cotes du Rhone Reserve Blanc",
                "price": "45.00",
                "url": "https://grandcruwines.com/products/2024-famille-perrin-cotes-du-rhone-reserve-blanc",
                "in_stock": "false",
            }
        ],
        enforce_in_stock=False,
    )
    cache = {
        cache_key(comparison_rows[0]["name_plat"]): {
            "resolved_at": time.time(),
            "suggestion": {
                "decision": "match",
                "match_name": "2024 Famille Perrin - Cotes du Rhone Reserve Blanc",
                "match_url": "https://grandcruwines.com/products/2024-famille-perrin-cotes-du-rhone-reserve-blanc",
                "confidence": 0.91,
                "reason": "Same wine and vintage.",
            },
        }
    }

    updated_rows, matched_count = resolve_rows(
        comparison_rows,
        grandcru_rows,
        api_key="test",
        max_candidates=8,
        min_confidence=0.75,
        cache=cache,
        force=False,
    )

    assert matched_count == 1
    assert updated_rows[0]["url_main"] == "https://grandcruwines.com/products/2024-famille-perrin-cotes-du-rhone-reserve-blanc"
    assert updated_rows[0]["cheaper_side"] == "Same Price"
    assert updated_rows[0]["price_main"] == "45.00"
    assert updated_rows[0]["platinum_in_stock"] == "true"
    assert updated_rows[0]["grand_cru_in_stock"] == "false"
    assert updated_rows[0]["image_url"] == "https://8362297.app.netsuite.com/core/media/media.nl?id=1"


def test_apply_match_scales_bundle_prices() -> None:
    row = {
        "quantity_plat": "3",
        "price_plat": "600.00",
        "quantity_main": "",
        "price_main": "",
        "price_diff": "",
        "price_diff_pct": "",
        "cheaper_side": "No Match",
        "url_main": "",
    }
    candidate = type("CandidateStub", (), {})()
    candidate.quantity = 1
    candidate.price = "210.00"
    candidate.url = "https://grandcruwines.com/products/example"
    candidate.in_stock = "false"

    updated = _apply_match(row, candidate)

    assert updated["quantity_main"] == "1"
    assert updated["price_main"] == "630.00"
    assert updated["cheaper_side"] == "Platinum Cheaper"
    assert updated["grand_cru_in_stock"] == "false"


def test_comparison_fieldnames_preserve_availability_and_extra_columns() -> None:
    fields = comparison_fieldnames(
        [
            {
                "name_plat": "Example",
                "url_main": "",
                "platinum_in_stock": "true",
                "image_url": "https://8362297.app.netsuite.com/core/media/media.nl?id=1",
                "custom_review_column": "keep",
            }
        ]
    )

    assert "platinum_in_stock" in fields
    assert "grand_cru_in_stock" in fields
    assert "image_url" in fields
    assert "custom_review_column" in fields
