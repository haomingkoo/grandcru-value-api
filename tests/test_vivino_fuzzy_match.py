from scripts.import_wine_data import build_vivino_lookup, match_vivino_row


def _row(name: str) -> dict[str, str]:
    return {"match_name": name, "wine_name": name, "vivino_url": "https://www.vivino.com/w/1", "vivino_rating": "4.0"}


def test_fuzzy_match_rejects_a_different_colour() -> None:
    lookup = build_vivino_lookup([_row("NV Felix Solis - Mucho Mas Gold - Red - 750 ml - Standard Bottle")])
    _, method = match_vivino_row("NV Felix Solis - Mucho Mas Blanco - White - 750 ml - Standard Bottle", lookup)
    assert method == "none"


def test_fuzzy_match_still_links_same_colour_bundle() -> None:
    lookup = build_vivino_lookup([_row("2022 The Hilt - Bentrock Chardonnay - White - 750 ml - Standard Bottle")])
    row, method = match_vivino_row(
        "2022 The Hilt - Bentrock Chardonnay - White - 750 ml - Standard Bottle (Bundle of 3)", lookup
    )
    assert method == "fuzzy"
    assert row["vivino_url"] == "https://www.vivino.com/w/1"
