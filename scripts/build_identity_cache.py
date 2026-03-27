"""One-time script to seed identity_cache.json from existing resolver data.

Reads validated URLs from:
1. vivino_overrides.csv (highest trust — manual/auto-accepted)
2. llm_vivino_cache.json (has rating = was successfully scraped)
3. market_price_cache.json (has validated Wine-Searcher URLs)

Usage:
    python scripts/build_identity_cache.py
    python scripts/build_identity_cache.py --dry-run
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.llm_utils import (  # noqa: E402
    IDENTITY_CACHE_PATH,
    cache_key,
    load_cache,
    load_identity_cache,
    save_identity_cache,
    set_identity,
)
from scripts.validate_market_prices import validate_row  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed identity cache from existing data")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    identity = load_identity_cache()
    added = 0

    # --- Source 1: vivino_overrides.csv (highest trust) ---
    overrides_path = ROOT / "seed" / "vivino_overrides.csv"
    if overrides_path.exists():
        with overrides_path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                name = row.get("match_name", "").strip()
                url = row.get("vivino_url", "").strip()
                if name and url and "vivino.com" in url:
                    key = cache_key(name)
                    if key not in identity:
                        set_identity(
                            identity, name,
                            vivino_url=url,
                            source="vivino_overrides",
                            validated=True,
                        )
                        added += 1
        print(f"[seed] vivino_overrides.csv: {added} entries added")

    # --- Source 2: llm_vivino_cache.json ---
    llm_cache_path = ROOT / "data" / "llm_vivino_cache.json"
    llm_cache = load_cache(llm_cache_path)
    llm_added = 0
    for key, entry in llm_cache.items():
        url = entry.get("vivino_url", "").strip()
        name = entry.get("wine_name", "").strip()
        if not name or not url or "vivino.com" not in url:
            continue
        # Only trust entries that have a rating (= successfully scraped)
        if not entry.get("vivino_rating"):
            continue
        if key not in identity:
            set_identity(
                identity, name,
                vivino_url=url,
                source="llm_vivino_cache",
                validated=True,
            )
            llm_added += 1
    print(f"[seed] llm_vivino_cache.json: {llm_added} entries added")
    added += llm_added

    # --- Source 3: market_price_cache.json (Wine-Searcher URLs) ---
    market_cache_path = ROOT / "data" / "market_price_cache.json"
    market_cache = load_cache(market_cache_path)
    market_added = 0
    for key, entry in market_cache.items():
        url = entry.get("retailer_url", "").strip()
        name = entry.get("match_name", "").strip() or key
        if not url or "wine-searcher.com" not in url:
            continue
        # Validate the URL match before trusting it
        issues = validate_row(name, url)
        if issues:
            continue
        # Merge Wine-Searcher URL into existing identity entry
        existing = identity.get(cache_key(name))
        if existing:
            existing["wine_searcher_url"] = url
        else:
            set_identity(
                identity, name,
                wine_searcher_url=url,
                source="market_price_cache",
                validated=True,
            )
            market_added += 1
    print(f"[seed] market_price_cache.json: {market_added} entries added")
    added += market_added

    # --- Also merge Wine-Searcher URLs from current seed/market_prices.csv ---
    market_csv = ROOT / "seed" / "market_prices.csv"
    if market_csv.exists():
        csv_added = 0
        with market_csv.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                name = row.get("match_name", "").strip()
                url = row.get("retailer_url", "").strip()
                if not name or not url:
                    continue
                key = cache_key(name)
                existing = identity.get(key)
                if existing and not existing.get("wine_searcher_url"):
                    existing["wine_searcher_url"] = url
                    csv_added += 1
        print(f"[seed] market_prices.csv: {csv_added} Wine-Searcher URLs merged")

    total = len(identity)
    vivino_count = sum(1 for e in identity.values() if e.get("vivino_url"))
    ws_count = sum(1 for e in identity.values() if e.get("wine_searcher_url"))
    print(f"\n[seed] Identity cache: {total} wines | {vivino_count} Vivino URLs | {ws_count} Wine-Searcher URLs")

    if args.dry_run:
        print("[dry-run] Would write to", IDENTITY_CACHE_PATH)
    else:
        save_identity_cache(identity)
        print(f"[seed] Saved to {IDENTITY_CACHE_PATH}")


if __name__ == "__main__":
    main()
