
from scripts.build_comparison_summary import build_matches, build_summary, prepare_rows
from scripts.import_wine_data import _fill_from_platinum_listing
from scripts.scrape_sources import parse_platinum_origin, parse_vivino_badge_rating
from app.wine_metadata import DerivedWineMetadata


def test_badge_rating_parses_plain_number() -> None:
    assert parse_vivino_badge_rating("4.1") == "4.1"
    assert parse_vivino_badge_rating(" 4.0 ") == "4"
    assert parse_vivino_badge_rating("") == ""
    assert parse_vivino_badge_rating("New") == ""
    assert parse_vivino_badge_rating("7.5") == ""


def test_origin_with_region() -> None:
    assert parse_platinum_origin(["France", ", Champagne", ", Pinot Noir"]) == {
        "platinum_country": "France",
        "platinum_region": "Champagne",
        "platinum_grapes": "Pinot Noir",
    }


def test_origin_without_region_and_blend_and_usa_alias() -> None:
    assert parse_platinum_origin(["USA", ", Proprietary Blend"]) == {
        "platinum_country": "United States",
        "platinum_region": "",
        "platinum_grapes": "",
    }
    assert parse_platinum_origin(["France"]) == {}


def test_comparison_passes_platinum_origin_through() -> None:
    platinum = prepare_rows(
        [
            {
                "name": "2023 Tenuta Perano - Chianti Classico - Red - 750 ml - Standard Bottle",
                "price": "48.00",
                "url": "https://platwineclub.wineportal.com/wines/2023-tenuta-perano-chianti-classico",
                "in_stock": "true",
                "platinum_vivino_rating": "3.9",
                "platinum_country": "Italy",
                "platinum_region": "Tuscany",
                "platinum_grapes": "Sangiovese",
            }
        ],
        enforce_in_stock=True,
    )
    summary = build_summary(build_matches([], platinum, threshold=0.6))
    assert summary[0]["platinum_country"] == "Italy"
    assert summary[0]["platinum_region"] == "Tuscany"
    assert summary[0]["platinum_grapes"] == "Sangiovese"
    assert summary[0]["platinum_vivino_rating"] == "3.9"


def test_listing_fills_only_missing_metadata() -> None:
    empty = DerivedWineMetadata(producer="Tenuta Perano")
    row = {"platinum_country": "Italy", "platinum_region": "Tuscany", "platinum_grapes": "Sangiovese"}
    filled = _fill_from_platinum_listing(empty, row)
    assert (filled.country, filled.region, filled.grapes) == ("Italy", "Tuscany", "Sangiovese")
    assert filled.origin_label == "Tuscany, Italy"
    assert filled.origin_source == "platinum_listing"

    known = DerivedWineMetadata(country="France", region="Burgundy", grapes="Pinot Noir")
    assert _fill_from_platinum_listing(known, row) == known
