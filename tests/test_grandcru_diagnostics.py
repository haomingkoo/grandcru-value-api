import time

from scripts.build_comparison_summary import prepare_rows
from scripts.diagnose_grandcru_matches import diagnose_rows
from scripts.llm_utils import cache_key


def test_diagnostics_explain_no_candidate_rows() -> None:
    comparison_rows = [
        {
            "name_plat": "2024 Example Estate - Missing Wine - White - 750 ml - Standard Bottle",
            "year_plat": "2024",
            "quantity_plat": "1",
            "volume_plat": "750ml",
            "price_plat": "45.00",
            "url_plat": "https://platwineclub.wineportal.com/wines/2024-example-estate-missing-wine-white-750-ml-standard-bottle",
            "url_main": "",
        }
    ]
    grandcru_rows = prepare_rows([], enforce_in_stock=True)

    report_rows = diagnose_rows(
        comparison_rows,
        grandcru_rows,
        {},
        max_candidates=8,
        min_confidence=0.75,
    )

    assert report_rows[0]["reason"] == "no_same_year_volume_candidate"
    assert report_rows[0]["candidate_count"] == "0"


def test_diagnostics_explain_low_confidence_llm_suggestion() -> None:
    wine_name = "2024 Famille Perrin - Cotes du Rhone Reserve Blanc - White - 750 ml - Standard Bottle"
    comparison_rows = [
        {
            "name_plat": wine_name,
            "year_plat": "2024",
            "quantity_plat": "1",
            "volume_plat": "750ml",
            "price_plat": "45.00",
            "url_plat": "https://platwineclub.wineportal.com/wines/2024-famille-perrin-cotes-du-rhone-reserve-blanc-white-750-ml-standard-bottle",
            "url_main": "",
        }
    ]
    grandcru_rows = prepare_rows(
        [
            {
                "name": "2024 Famille Perrin - Cotes du Rhone Reserve Blanc",
                "price": "45.00",
                "url": "https://grandcruwines.com/products/2024-famille-perrin-cotes-du-rhone-reserve-blanc",
                "in_stock": "true",
            }
        ],
        enforce_in_stock=True,
    )
    cache = {
        cache_key(wine_name): {
            "resolved_at": time.time(),
            "suggestion": {
                "decision": "match",
                "match_url": "https://grandcruwines.com/products/2024-famille-perrin-cotes-du-rhone-reserve-blanc",
                "confidence": 0.5,
            },
        }
    }

    report_rows = diagnose_rows(
        comparison_rows,
        grandcru_rows,
        cache,
        max_candidates=8,
        min_confidence=0.75,
    )

    assert report_rows[0]["reason"] == "llm_low_confidence"
    assert report_rows[0]["candidate_count"] == "1"
    assert report_rows[0]["llm_confidence"] == "0.500"
