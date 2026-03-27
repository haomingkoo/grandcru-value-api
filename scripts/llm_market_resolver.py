"""Market price resolver using Brave Search + Wine-Searcher.

For each wine, uses Brave Search API to find the Wine-Searcher page
and extracts the average market price from the search snippet.
No Gemini/LLM needed — Brave snippets contain the price directly.

Usage:
    python scripts/llm_market_resolver.py
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
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.llm_utils import (  # noqa: E402
    cache_key,
    is_cache_fresh,
    load_cache,
    save_cache,
)
from scripts.validate_market_prices import validate_row  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("grandcru.market_resolver")

CACHE_TTL_DAYS = 30
_DEFAULT_USD_TO_SGD = 1.30


def _fetch_usd_to_sgd() -> float:
    """Fetch live USD→SGD rate, fall back to default on error."""
    try:
        req = Request(
            "https://open.er-api.com/v6/latest/USD",
            headers={"Accept": "application/json"},
        )
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        rate = data.get("rates", {}).get("SGD")
        if rate and isinstance(rate, (int, float)) and 1.0 < rate < 2.0:
            logger.info("Live USD→SGD rate: %.4f", rate)
            return round(rate, 4)
    except Exception as exc:
        logger.warning("Failed to fetch exchange rate: %s", exc)
    logger.info("Using default USD→SGD rate: %.2f", _DEFAULT_USD_TO_SGD)
    return _DEFAULT_USD_TO_SGD

_OUTPUT_FIELDS = [
    "match_name",
    "retailer_name",
    "retailer_url",
    "price_sgd",
    "currency_confirmed",
    "notes",
]


def _clean_wine_name_for_search(name: str) -> str:
    """Strip Platinum-specific formatting for a cleaner search query.

    Transforms: "2021 Chateau Beaucastel - Chateauneuf du Pape - Red - 750 ml - Standard Bottle (Bundle of 6)"
    Into: "Chateau Beaucastel Chateauneuf du Pape 2021"
    """
    cleaned = re.sub(
        r"\s*-\s*(Red|White|Rose|Rosé|Sparkling)\s*-.*$", "", name,
    )
    cleaned = re.sub(r"\(.*?\)", "", cleaned)
    cleaned = cleaned.replace(" - ", " ").strip()
    vintage_match = re.match(r"^((?:NV|\d{4}))\s+(.+)", cleaned)
    if vintage_match:
        vintage, rest = vintage_match.groups()
        cleaned = f"{rest.strip()} {vintage}"
    return cleaned.strip()


def resolve_market_price(
    wine_name: str,
    brave_api_key: str,
    *,
    usd_to_sgd: float = _DEFAULT_USD_TO_SGD,
) -> dict[str, str]:
    """Search Brave for Wine-Searcher page, extract price from snippet."""
    result: dict[str, str] = {f: "" for f in _OUTPUT_FIELDS}
    result["match_name"] = wine_name

    search_name = _clean_wine_name_for_search(wine_name)
    query = f"site:wine-searcher.com {search_name} average price"
    url = (
        f"https://api.search.brave.com/res/v1/web/search"
        f"?q={quote_plus(query)}&count=5"
    )

    try:
        req = Request(url, headers={
            "Accept": "application/json",
            "X-Subscription-Token": brave_api_key,
        })
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as exc:
        result["notes"] = f"brave_error={exc}"
        return result

    web_results = data.get("web", {}).get("results", [])

    # Key words from search name for validation
    search_words = set(
        w.lower() for w in re.findall(r"[a-zA-Z]{3,}", search_name)
        if w.lower() not in ("wine", "the", "and", "red", "white", "rose")
    )

    for r in web_results:
        ws_url = r.get("url", "")
        title = r.get("title", "")
        desc = r.get("description", "")

        if "wine-searcher.com" not in ws_url:
            continue

        text = f"{title} {desc}"

        # Validate: at least half the key words appear in the result
        result_lower = text.lower()
        matched_words = sum(1 for w in search_words if w in result_lower)
        if search_words and matched_words < len(search_words) * 0.4:
            continue  # Wrong wine

        # Extract average price from snippet (USD)
        # Prefer "Avg Price" pattern, fall back to any dollar amount
        avg_match = re.search(
            r"Avg Price[^$]*\$\s*(\d+(?:\.\d{2})?)", text,
        )
        if avg_match:
            prices = [float(avg_match.group(1))]
        else:
            price_matches = re.findall(
                r"\$\s*(\d+(?:\.\d{2})?)", text,
            )
            prices = [
                float(p) for p in price_matches
                if 5 <= float(p) <= 5000
            ]

        if not prices:
            continue

        avg_usd = prices[0]

        # Sanity check: reject if price seems wildly off
        # (e.g., Wine-Searcher matched a different wine/vintage)
        if avg_usd > 500:
            result["notes"] = f"price_outlier usd={avg_usd}"
            continue

        avg_sgd = round(avg_usd * usd_to_sgd, 2)

        # Extract store count if available
        store_match = re.search(
            r"(\d+)\s+(?:stores?|shops?|merchants?|offers?)", text,
        )
        stores = int(store_match.group(1)) if store_match else None
        stores_str = f" ({stores} stores)" if stores else ""

        result["retailer_name"] = f"Wine-Searcher avg{stores_str}"
        result["retailer_url"] = ws_url
        result["price_sgd"] = str(avg_sgd)
        result["currency_confirmed"] = "true"
        result["notes"] = f"resolved_via=brave_wine_searcher usd={avg_usd}"
        return result

    result["notes"] = "no_wine_searcher_match"
    return result


def _build_retry_query(wine_name: str) -> str | None:
    """Build a tighter search query for wines that failed validation.

    Uses the parsed producer + label directly, and adds exclusions
    for common mis-classifications (e.g., exclude 'premier cru' for
    village-level wines).
    """
    parts = [p.strip() for p in wine_name.split(" - ")]
    colours = {"red", "white", "rose", "rosé"}

    # Find colour position to split producer from label
    colour_idx = None
    for i, p in enumerate(parts):
        if p.lower() in colours:
            colour_idx = i
            break

    if colour_idx and colour_idx >= 2:
        producer_parts = parts[0:colour_idx - 1]
        producer_raw = " ".join(producer_parts)
        label = parts[colour_idx - 1]
    elif len(parts) >= 2:
        producer_raw = parts[0]
        label = parts[1] if parts[1].lower() not in colours else ""
    else:
        return None

    # Strip vintage from producer
    vintage_match = re.match(r"^(NV|\d{4})\s+(.+)", producer_raw)
    vintage = ""
    if vintage_match:
        vintage = vintage_match.group(1)
        producer_raw = vintage_match.group(2)

    producer = producer_raw.strip()
    label = label.strip()
    lower = wine_name.lower()

    # Build query: producer + label + vintage
    query_parts = [producer, label]
    if vintage and vintage != "NV":
        query_parts.append(vintage)

    # Add exclusions for classification mismatches
    exclusions = []
    if "premier cru" not in lower and "1er cru" not in lower:
        exclusions.append("-\"premier cru\"")
    if "grand cru" not in lower:
        exclusions.append("-\"grand cru\"")

    query = f"site:wine-searcher.com {' '.join(query_parts)} average price"
    if exclusions:
        query += " " + " ".join(exclusions)

    return query


def resolve_with_validation(
    wine_name: str,
    brave_api_key: str,
    *,
    usd_to_sgd: float = _DEFAULT_USD_TO_SGD,
    sleep_between: float = 0.5,
) -> dict[str, str]:
    """Resolve → validate → retry with tighter query → validate again.

    Returns the result dict with an extra 'validation' field:
    'passed', 'passed_retry', or 'tried_failed'.
    """
    # Attempt 1: standard query
    result = resolve_market_price(wine_name, brave_api_key, usd_to_sgd=usd_to_sgd)

    if result.get("retailer_url"):
        issues = validate_row(wine_name, result["retailer_url"])
        if not issues:
            result["notes"] += " validation=passed"
            return result
        logger.info("  Attempt 1 failed validation: %s", "; ".join(issues))
    else:
        issues = ["no URL returned"]

    # Attempt 2: tighter query with exclusions
    time.sleep(sleep_between)
    retry_query = _build_retry_query(wine_name)
    if not retry_query:
        result["notes"] += " validation=tried_failed"
        return result

    logger.info("  Retrying with: %s", retry_query[:80])

    retry_url = (
        f"https://api.search.brave.com/res/v1/web/search"
        f"?q={quote_plus(retry_query)}&count=5"
    )

    try:
        req = Request(retry_url, headers={
            "Accept": "application/json",
            "X-Subscription-Token": brave_api_key,
        })
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as exc:
        result["notes"] += f" retry_error={exc} validation=tried_failed"
        return result

    web_results = data.get("web", {}).get("results", [])

    search_words = set(
        w.lower() for w in re.findall(r"[a-zA-Z]{3,}", retry_query)
        if w.lower() not in (
            "wine", "the", "and", "red", "white", "rose",
            "site", "searcher", "com", "average", "price",
            "premier", "grand", "cru",
        )
    )

    for r in web_results:
        ws_url = r.get("url", "")
        title = r.get("title", "")
        desc = r.get("description", "")

        if "wine-searcher.com" not in ws_url:
            continue

        text = f"{title} {desc}"

        # Validate URL first before extracting price
        url_issues = validate_row(wine_name, ws_url)
        if url_issues:
            continue  # Skip results that fail validation

        # Extract price
        avg_match = re.search(r"Avg Price[^$]*\$\s*(\d+(?:\.\d{2})?)", text)
        if avg_match:
            prices = [float(avg_match.group(1))]
        else:
            price_matches = re.findall(r"\$\s*(\d+(?:\.\d{2})?)", text)
            prices = [float(p) for p in price_matches if 5 <= float(p) <= 5000]

        if not prices:
            continue

        avg_usd = prices[0]
        avg_sgd = round(avg_usd * usd_to_sgd, 2)

        store_match = re.search(r"(\d+)\s+(?:stores?|shops?|merchants?|offers?)", text)
        stores = int(store_match.group(1)) if store_match else None
        stores_str = f" ({stores} stores)" if stores else ""

        result["retailer_name"] = f"Wine-Searcher avg{stores_str}"
        result["retailer_url"] = ws_url
        result["price_sgd"] = str(avg_sgd)
        result["currency_confirmed"] = "true"
        result["notes"] = f"resolved_via=brave_wine_searcher_retry usd={avg_usd} validation=passed_retry"
        return result

    # Both attempts failed
    result["notes"] += " validation=tried_failed"
    logger.info("  Both attempts failed validation — marking tried_failed")
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
        description="Market price resolver via Brave Search + Wine-Searcher",
    )
    parser.add_argument(
        "--comparison", default="seed/comparison_summary.csv",
    )
    parser.add_argument(
        "--cache-file", default="data/market_price_cache.json",
    )
    parser.add_argument("--output", default="data/market_prices.csv")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true", help="Ignore cache")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sleep", type=float, default=0.5)
    parser.add_argument(
        "--cache-ttl-days", type=int, default=CACHE_TTL_DAYS,
    )
    parser.add_argument("--brave-api-key", default="")
    args = parser.parse_args()

    brave_key = (
        args.brave_api_key or os.getenv("BRAVE_API_KEY", "")
    )
    if not brave_key:
        logger.error("No Brave API key found (set BRAVE_API_KEY)")
        sys.exit(1)

    comparison_path = ROOT / args.comparison
    cache_path = ROOT / args.cache_file
    output_path = ROOT / args.output

    if not comparison_path.exists():
        logger.error("Comparison file not found: %s", comparison_path)
        sys.exit(1)

    with comparison_path.open("r", encoding="utf-8", newline="") as f:
        wines = list(csv.DictReader(f))

    from scripts.llm_utils import get_identity, load_identity_cache

    logger.info("Loaded %d wines from comparison", len(wines))

    # Fetch live exchange rate once
    usd_to_sgd = _fetch_usd_to_sgd()

    cache = load_cache(cache_path)
    identity_cache = load_identity_cache()
    results: list[dict[str, str]] = []
    resolved_count = 0
    skipped_count = 0
    identity_hits = 0

    for i, wine in enumerate(wines):
        name = wine.get("name_plat", "")
        if not name:
            continue

        if args.limit and resolved_count >= args.limit:
            break

        key = cache_key(name)
        cached = cache.get(key, {})

        if not args.force and is_cache_fresh(cached, args.cache_ttl_days):
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

        # Check identity cache for known Wine-Searcher URL
        identity = get_identity(identity_cache, name)
        known_ws_url = identity.get("wine_searcher_url", "") if identity else ""

        if known_ws_url:
            identity_hits += 1
            logger.info(
                "[%d/%d] Refreshing price (known URL): %s",
                i + 1, len(wines), name[:60],
            )
            # Use known URL directly — single Brave call, no retry needed
            result = resolve_market_price(
                name, brave_key, usd_to_sgd=usd_to_sgd,
            )
            if result.get("price_sgd"):
                result["notes"] += " identity_cache_hit"
        else:
            logger.info(
                "[%d/%d] Resolving (new): %s",
                i + 1, len(wines), name[:60],
            )
            result = resolve_with_validation(
                name, brave_key,
                usd_to_sgd=usd_to_sgd,
                sleep_between=args.sleep,
            )
        results.append(result)

        cache[key] = {
            "match_name": name,
            "retailer_name": result.get("retailer_name", ""),
            "retailer_url": result.get("retailer_url", ""),
            "price_sgd": result.get("price_sgd", ""),
            "currency_confirmed": result.get("currency_confirmed", ""),
            "resolved_at": time.time(),
        }

        # Update identity cache with validated Wine-Searcher URL
        ws_url = result.get("retailer_url", "")
        if ws_url and "validation=passed" in result.get("notes", ""):
            from scripts.llm_utils import set_identity, save_identity_cache
            set_identity(
                identity_cache, name,
                wine_searcher_url=ws_url,
                source="market_resolver",
                validated=True,
            )

        if result.get("price_sgd"):
            resolved_count += 1
            logger.info(
                "  Found: %s → S$%s (%s)",
                name[:40], result["price_sgd"],
                result.get("retailer_name", "?"),
            )
        else:
            logger.info("  No market price: %s", name[:40])

        time.sleep(args.sleep)

    save_cache(cache_path, cache)
    save_identity_cache(identity_cache)

    if results:
        write_csv(output_path, results, _OUTPUT_FIELDS)
        logger.info(
            "Wrote %d results to %s (%d resolved, %d cached)",
            len(results), output_path, resolved_count, skipped_count,
        )

    total_with_price = sum(1 for r in results if r.get("price_sgd"))
    logger.info(
        "market_resolve_done total=%d with_price=%d cached=%d identity_hits=%d",
        len(results), total_with_price, skipped_count, identity_hits,
    )


if __name__ == "__main__":
    main()
