from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.ops import build_refresh_command
from scripts.import_wine_data import build_vivino_lookup, match_vivino_row
from scripts.llm_vivino_resolver import upsert_overrides


def test_weekly_refresh_command_omits_llm_resolver_flags() -> None:
    command = build_refresh_command(
        mode="weekly",
        health_url="https://example.com/health",
        strict_health=False,
    )

    assert "--llm-resolve" not in command
    assert "--llm-resolve-all" not in command


def test_exact_override_last_row_wins_for_blank_price_suppression() -> None:
    base_row = {
        "match_name": "2021 Test Wine",
        "wine_name": "2021 Test Wine",
        "vivino_rating": "4.0",
        "vivino_num_ratings": "10",
        "vivino_price": "999.0",
        "vivino_url": "https://vivino.com/w/1",
    }
    override_row = {
        "match_name": "2021 Test Wine",
        "wine_name": "2021 Test Wine",
        "vivino_rating": "4.0",
        "vivino_num_ratings": "10",
        "vivino_price": "",
        "vivino_url": "https://vivino.com/w/1",
        "notes": "manual suppress",
    }

    lookup = build_vivino_lookup([base_row, override_row])
    matched, method = match_vivino_row("2021 Test Wine", lookup)

    assert method == "exact"
    assert matched["vivino_price"] == ""
    assert matched["notes"] == "manual suppress"


def test_upsert_overrides_blank_price_clears_stale_price() -> None:
    existing = [
        {
            "match_name": "2021 Test Wine",
            "wine_name": "2021 Test Wine",
            "vivino_rating": "4.0",
            "vivino_num_ratings": "10",
            "vivino_price": "999.0",
            "vivino_description": "",
            "vivino_url": "https://vivino.com/w/1",
            "notes": "old bad price",
        }
    ]
    incoming = [
        {
            "match_name": "2021 Test Wine",
            "wine_name": "2021 Test Wine",
            "vivino_rating": "4.1",
            "vivino_num_ratings": "11",
            "vivino_price": "",
            "vivino_description": "",
            "vivino_url": "https://vivino.com/w/1",
            "notes": "fresh lookup with no price",
        }
    ]

    merged = upsert_overrides(existing, incoming)

    assert len(merged) == 1
    assert merged[0]["vivino_price"] == ""
    assert merged[0]["notes"] == "fresh lookup with no price"
