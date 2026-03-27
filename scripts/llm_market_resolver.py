"""Market price resolver using Gemini with Google Search grounding.

For each wine, searches Singapore wine retailers for prices (excluding
Platinum and Grand Cru). Uses the same Gemini grounding approach proven
for Vivino resolution.

Usage:
    python scripts/llm_market_resolver.py --auto-apply
    python scripts/llm_market_resolver.py --dry-run --limit 5
    python scripts/llm_market_resolver.py --force   # ignore cache
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.llm_utils import (  # noqa: E402
    _parse_grounding_json,
    cache_key,
    call_gemini_with_search,
    is_cache_fresh,
    load_cache,
    save_cache,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("grandcru.market_resolver")

CACHE_TTL_DAYS = 30

_OUTPUT_FIELDS = [
    "match_name",
    "retailer_name",
    "retailer_url",
    "price_sgd",
    "currency_confirmed",
    "notes",
]

# Retailers to exclude (we already compare these directly)
_EXCLUDED_DOMAINS = [
    "grandcruwines.com",
    "grandcru.com.sg",
    "platwineclub.wineportal.com",
    "vivino.com",
]


def _clean_wine_name_for_search(name: str) -> str:
    """Strip retailer-specific suffixes for a cleaner search query."""
    cleaned = re.sub(
        r"\s*-\s*(Red|White|Rose|Rosé|Sparkling)\s*-.*$", "", name,
    )
    return cleaned.strip()


def resolve_market_price(
    wine_name: str,
    api_key: str,
    *,
    sleep_seconds: float = 2.0,
) -> dict[str, str]:
    """Search Singapore retailers for a wine's price via Gemini grounding."""
    result: dict[str, str] = {f: "" for f in _OUTPUT_FIELDS}
    result["match_name"] = wine_name

    search_name = _clean_wine_name_for_search(wine_name)
    exclude_list = ", ".join(_EXCLUDED_DOMAINS)

    prompt = (
        f'Search for this wine available to buy in Singapore: "{search_name}"\n'
        f"Find the price in SGD from a Singapore wine retailer.\n"
        f"Exclude these sites: {exclude_list}\n"
        f"Good retailers include: wineculture.com.sg, wine.delivery, "
        f"1855thebottleshop.com, wineconnection.com.sg, ewineasia.com\n"
        f"Return ONLY a JSON object (no markdown) with keys:\n"
        f"retailer_name (string), retailer_url (full product page URL), "
        f"price_sgd (number in Singapore dollars), "
        f"currency_confirmed (boolean, true if confident price is SGD).\n"
        f"Only include data you found via search. Do not fabricate."
    )

    raw = call_gemini_with_search(prompt, api_key)
    data = _parse_grounding_json(raw)

    if not data:
        result["notes"] = "grounding_parse_failed"
        return result

    # Validate the result
    price = data.get("price_sgd")
    retailer = data.get("retailer_name", "")
    url = data.get("retailer_url", "")
    confirmed = data.get("currency_confirmed", False)

    # Check that the retailer isn't one of our excluded domains
    if url:
        for domain in _EXCLUDED_DOMAINS:
            if domain in url.lower():
                logger.info(
                    "Excluded retailer %s for '%s', skipping",
                    domain, wine_name,
                )
                result["notes"] = f"excluded_retailer={domain}"
                return result

    if price and isinstance(price, (int, float)) and price > 0:
        result["retailer_name"] = str(retailer)
        result["retailer_url"] = str(url)
        result["price_sgd"] = str(round(float(price), 2))
        result["currency_confirmed"] = str(confirmed).lower()
        result["notes"] = "resolved_via=grounding"
    else:
        result["notes"] = "no_price_found"

    return result


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    """Write rows to a CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Market price resolver via Gemini Search grounding",
    )
    parser.add_argument(
        "--comparison", default="seed/comparison_summary.csv",
        help="Path to comparison_summary.csv",
    )
    parser.add_argument(
        "--cache-file", default="data/market_price_cache.json",
    )
    parser.add_argument(
        "--output", default="data/market_prices.csv",
    )
    parser.add_argument("--auto-apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="Ignore cache")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sleep", type=float, default=3.0)
    parser.add_argument(
        "--cache-ttl-days", type=int, default=CACHE_TTL_DAYS,
    )
    parser.add_argument("--gemini-api-key", default="")
    args = parser.parse_args()

    api_key = (
        args.gemini_api_key
        or os.getenv("GEMINI_API_KEY", "")
        or os.getenv("GOOGLE_API_KEY", "")
    )
    if not api_key:
        logger.error("No Gemini API key found")
        sys.exit(1)

    comparison_path = ROOT / args.comparison
    cache_path = ROOT / args.cache_file
    output_path = ROOT / args.output

    # Load comparison wines
    if not comparison_path.exists():
        logger.error("Comparison file not found: %s", comparison_path)
        sys.exit(1)

    with comparison_path.open("r", encoding="utf-8", newline="") as f:
        wines = list(csv.DictReader(f))

    logger.info("Loaded %d wines from comparison", len(wines))

    # Load cache
    cache = load_cache(cache_path)
    results: list[dict[str, str]] = []
    resolved_count = 0
    skipped_count = 0

    for i, wine in enumerate(wines):
        name = wine.get("name_plat", "")
        if not name:
            continue

        if args.limit and resolved_count >= args.limit:
            break

        key = cache_key(name)
        cached = cache.get(key, {})

        if not args.force and is_cache_fresh(cached, args.cache_ttl_days):
            # Use cached result
            results.append({
                "match_name": name,
                "retailer_name": cached.get("retailer_name", ""),
                "retailer_url": cached.get("retailer_url", ""),
                "price_sgd": cached.get("price_sgd", ""),
                "currency_confirmed": cached.get("currency_confirmed", ""),
                "notes": f"cached resolved_at={cached.get('resolved_at', '')}",
            })
            skipped_count += 1
            continue

        if args.dry_run:
            search_name = _clean_wine_name_for_search(name)
            logger.info("[DRY RUN] Would search: %s", search_name)
            continue

        logger.info(
            "[%d/%d] Resolving market price: %s",
            i + 1, len(wines), name[:60],
        )

        result = resolve_market_price(name, api_key, sleep_seconds=args.sleep)
        results.append(result)

        # Update cache
        cache_entry = {
            "retailer_name": result.get("retailer_name", ""),
            "retailer_url": result.get("retailer_url", ""),
            "price_sgd": result.get("price_sgd", ""),
            "currency_confirmed": result.get("currency_confirmed", ""),
            "resolved_at": time.time(),
        }
        cache[key] = cache_entry

        if result.get("price_sgd"):
            resolved_count += 1
            logger.info(
                "  Found: %s at S$%s from %s",
                name[:40],
                result["price_sgd"],
                result.get("retailer_name", "?"),
            )
        else:
            logger.info("  No market price found for: %s", name[:40])

        time.sleep(args.sleep)

    # Save cache
    save_cache(cache_path, cache)

    # Write output CSV
    if results:
        write_csv(output_path, results, _OUTPUT_FIELDS)
        logger.info(
            "Wrote %d results to %s (%d resolved, %d cached)",
            len(results), output_path, resolved_count, skipped_count,
        )

    # Summary
    total_with_price = sum(1 for r in results if r.get("price_sgd"))
    logger.info(
        "market_resolve_done total=%d with_price=%d cached=%d",
        len(results), total_with_price, skipped_count,
    )


if __name__ == "__main__":
    main()
