"""LLM-powered Vivino resolver using Gemini Flash.

For each unmatched wine, asks Gemini to extract structured identity and
generate the best Vivino search query. Then searches Vivino directly
and fetches rating + review count from the wine page.

Results are cached with a 30-day TTL — only new or stale wines trigger
Gemini + Vivino calls.

Usage:
    python scripts/llm_vivino_resolver.py --all --auto-apply
    python scripts/llm_vivino_resolver.py --dry-run --limit 5
    python scripts/llm_vivino_resolver.py --force              # ignore cache
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

from scripts.import_wine_data import (  # noqa: E402
    build_vivino_lookup,
    canonicalize_key,
    match_vivino_row,
    read_csv_rows,
    read_optional_csv_rows,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("grandcru.llm_resolver")

CACHE_TTL_DAYS = 30

_OVERRIDE_FIELDS = [
    "match_name",
    "wine_name",
    "vivino_rating",
    "vivino_num_ratings",
    "vivino_price",
    "vivino_url",
    "notes",
]

_AGG_RATING_RE = re.compile(
    r'"aggregateRating"\s*:\s*\{[^}]*"ratingValue"\s*:\s*"?(?P<rating>[\d.]+)"?'
    r'[^}]*"ratingCount"\s*:\s*"?(?P<count>[\d,]+)"?',
    re.DOTALL,
)
_RATING_VALUE_RE = re.compile(r'"ratingValue"\s*:\s*"?(?P<rating>[\d.]+)"?')
_RATING_COUNT_RE = re.compile(r'"ratingCount"\s*:\s*"?(?P<count>[\d,]+)"?')
_REVIEW_COUNT_RE = re.compile(r'"reviewCount"\s*:\s*"?(?P<count>[\d,]+)"?')
_VIVINO_WINE_URL_RE = re.compile(r'href="(/[a-z]{2}/en/[^"]+/w/\d+[^"]*)"')
_VIVINO_WINE_URL_FULL_RE = re.compile(
    r"https?://(?:www\.)?vivino\.com/[^\s\"'<>]*?/w/\d+"
)


# ── Cache ──────────────────────────────────────────────────────────────


def load_cache(path: Path) -> dict[str, dict]:
    """Load the LLM resolver cache from JSON."""
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_cache(path: Path, cache: dict[str, dict]) -> None:
    """Persist the cache to JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def cache_key(wine_name: str) -> str:
    """Stable cache key from a wine name."""
    return canonicalize_key(wine_name)


def is_cache_fresh(entry: dict, ttl_days: int) -> bool:
    """Check if a cache entry is within its TTL."""
    resolved_at = entry.get("resolved_at")
    if not isinstance(resolved_at, (int, float)):
        return False
    age_days = (time.time() - resolved_at) / 86400
    return age_days < ttl_days


# ── Gemini API ─────────────────────────────────────────────────────────


def call_gemini(prompt: str, api_key: str, model: str = "gemini-2.5-flash") -> str:
    """Call Gemini API via REST with thinking disabled for fast, clean output."""
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent"
    )
    body = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 512,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }).encode("utf-8")
    req = Request(url, data=body, method="POST", headers={
        "Content-Type": "application/json",
        "x-goog-api-key": api_key,
    })
    with urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    candidates = data.get("candidates", [])
    if not candidates:
        return ""
    parts = candidates[0].get("content", {}).get("parts", [])
    return parts[0].get("text", "") if parts else ""


def extract_vivino_query(wine_name: str, api_key: str) -> dict:
    """Ask Gemini to parse a retailer wine name into structured data."""
    prompt = f"""You are a wine expert. Given this wine listing from a retailer:

"{wine_name}"

Extract the following and respond ONLY with valid JSON (no markdown, no code fences):
{{
  "producer": "producer/winery name",
  "wine": "wine or cuvee name",
  "appellation": "appellation or region",
  "vintage": "year or NV",
  "grape": "grape variety if apparent, else empty",
  "vivino_query": "the 3-5 most essential search terms to find this exact wine on Vivino"
}}

For vivino_query, use just: producer + wine name + vintage. Drop color, bottle size, packaging."""
    raw = call_gemini(prompt, api_key)
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning("Gemini parse failed for '%s': %s", wine_name, raw[:200])
        return {}


# ── Vivino fetching ────────────────────────────────────────────────────


def fetch_html(url: str, timeout: int = 20) -> str:
    """Fetch a URL and return HTML content."""
    req = Request(url, headers={
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    })
    with urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def parse_vivino_rating(html: str) -> tuple[str, str]:
    """Extract rating and count from Vivino page HTML via JSON-LD."""
    match = _AGG_RATING_RE.search(html)
    if match:
        return match.group("rating"), match.group("count").replace(",", "")
    rating, count = "", ""
    m_rating = _RATING_VALUE_RE.search(html)
    if m_rating:
        rating = m_rating.group("rating")
    m_count = _RATING_COUNT_RE.search(html) or _REVIEW_COUNT_RE.search(html)
    if m_count:
        count = m_count.group("count").replace(",", "")
    return rating, count


def search_vivino_via_brave(query: str, brave_api_key: str) -> str | None:
    """Use Brave Search API to find the Vivino wine page URL."""
    search_query = f"{query} site:vivino.com"
    params = f"q={quote_plus(search_query)}&count=5"
    url = f"https://api.search.brave.com/res/v1/web/search?{params}"
    req = Request(url, headers={
        "Accept": "application/json",
        "X-Subscription-Token": brave_api_key,
    })
    try:
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.warning("Brave search failed for '%s': %s", query, exc)
        return None

    for result in (data.get("web") or {}).get("results", []):
        link = (result.get("url") or "").strip()
        if "vivino.com" in link and "/w/" in link:
            # Clean URL: keep path up to wine ID, strip query params.
            clean = link.split("?")[0].rstrip("/")
            return clean

    return None


def search_vivino_for_url(query: str, brave_api_key: str = "") -> str | None:
    """Find a Vivino wine URL — uses Brave Search API if key available."""
    if brave_api_key:
        return search_vivino_via_brave(query, brave_api_key)

    # Fallback: direct Vivino search (works only if page isn't JS-rendered).
    search_url = f"https://www.vivino.com/en/search/wines?q={quote_plus(query)}"
    try:
        html = fetch_html(search_url)
    except Exception as exc:
        logger.warning("Vivino search failed for '%s': %s", query, exc)
        return None

    matches = _VIVINO_WINE_URL_RE.findall(html)
    if matches:
        return f"https://www.vivino.com{matches[0].split('?')[0]}"

    full_matches = _VIVINO_WINE_URL_FULL_RE.findall(html)
    if full_matches:
        return full_matches[0].split("?")[0]

    return None


# ── Resolution ─────────────────────────────────────────────────────────


def resolve_wine(
    wine_name: str,
    api_key: str,
    *,
    brave_api_key: str = "",
    dry_run: bool = False,
    sleep_seconds: float = 2.0,
) -> dict:
    """Resolve a single wine: LLM parse -> Vivino search -> fetch metrics."""
    result: dict[str, str] = {f: "" for f in _OVERRIDE_FIELDS}
    result["match_name"] = wine_name

    # Step 1: LLM parses the wine name.
    parsed = extract_vivino_query(wine_name, api_key)
    if not parsed:
        result["notes"] = "gemini_parse_failed"
        return result

    query = parsed.get("vivino_query", "").strip()
    producer = parsed.get("producer", "").strip()
    wine = parsed.get("wine", "").strip()
    vintage = parsed.get("vintage", "").strip()
    result["wine_name"] = f"{producer} {wine}".strip()
    result["notes"] = f"llm_query={query}"

    if not query:
        result["notes"] = "gemini_empty_query"
        return result

    if dry_run:
        result["notes"] = f"dry_run query={query}"
        return result

    # Step 2: Search for Vivino URL via Brave (or direct fallback).
    time.sleep(sleep_seconds)
    vivino_url = search_vivino_for_url(query, brave_api_key)

    if not vivino_url and vintage and vintage != "NV":
        fallback_query = f"{producer} {wine}".strip()
        if fallback_query != query:
            time.sleep(sleep_seconds)
            vivino_url = search_vivino_for_url(fallback_query, brave_api_key)
            result["notes"] += f" retry_query={fallback_query}"

    if not vivino_url:
        result["vivino_url"] = (
            f"https://www.vivino.com/en/search/wines?q={quote_plus(query)}"
        )
        result["notes"] += " no_direct_match"
        return result

    result["vivino_url"] = vivino_url

    # Step 3: Fetch the Vivino wine page for metrics.
    time.sleep(sleep_seconds)
    try:
        page_html = fetch_html(vivino_url)
        rating, count = parse_vivino_rating(page_html)
        result["vivino_rating"] = rating
        result["vivino_num_ratings"] = count
    except Exception as exc:
        result["notes"] += f" metric_fetch_error={exc}"

    return result


# ── CSV helpers ────────────────────────────────────────────────────────


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def upsert_overrides(
    existing: list[dict[str, str]],
    new_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    by_name: dict[str, dict[str, str]] = {}
    for row in existing:
        key = (row.get("match_name") or "").strip()
        if key:
            by_name[key] = row.copy()
    for row in new_rows:
        key = (row.get("match_name") or "").strip()
        if not key:
            continue
        prior = by_name.get(key, {}).copy()
        for field in _OVERRIDE_FIELDS:
            incoming = (row.get(field) or "").strip()
            if incoming:
                prior[field] = incoming
        prior["match_name"] = key
        by_name[key] = prior
    return sorted(by_name.values(), key=lambda r: r.get("match_name", ""))


# ── Main ───────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="LLM-powered Vivino resolver using Gemini Flash (with 30-day cache)",
    )
    parser.add_argument("--comparison", type=Path, default=ROOT / "seed" / "comparison_summary.csv")
    parser.add_argument("--vivino", type=Path, default=ROOT / "seed" / "vivino_results.csv")
    parser.add_argument("--vivino-overrides", type=Path, default=ROOT / "seed" / "vivino_overrides.csv")
    parser.add_argument("--cache-file", type=Path, default=ROOT / "data" / "llm_vivino_cache.json")
    parser.add_argument("--output", type=Path, default=ROOT / "data" / "llm_resolved.csv")
    parser.add_argument("--auto-apply", action="store_true", help="Merge results into vivino_overrides.csv")
    parser.add_argument("--dry-run", action="store_true", help="Show LLM queries without searching Vivino")
    parser.add_argument("--all", action="store_true", help="Resolve ALL wines, not just unmatched")
    parser.add_argument("--force", action="store_true", help="Ignore cache, re-resolve everything")
    parser.add_argument("--limit", type=int, default=0, help="Max wines to resolve (0 = all)")
    parser.add_argument("--sleep", type=float, default=2.0, help="Seconds between Vivino requests")
    parser.add_argument("--cache-ttl-days", type=int, default=CACHE_TTL_DAYS, help="Cache TTL in days")
    parser.add_argument(
        "--gemini-api-key",
        default=os.getenv("GEMINI_API_KEY", "") or os.getenv("GOOGLE_API_KEY", ""),
        help="Gemini API key (or set GEMINI_API_KEY / GOOGLE_API_KEY env var)",
    )
    parser.add_argument(
        "--brave-api-key",
        default=os.getenv("BRAVE_API_KEY", ""),
        help="Brave Search API key for finding Vivino URLs (or set BRAVE_API_KEY env var)",
    )
    args = parser.parse_args()

    if not args.gemini_api_key:
        print("Error: No API key found. Set GEMINI_API_KEY or GOOGLE_API_KEY in .env")
        print("Get a free key at: https://aistudio.google.com/apikey")
        sys.exit(1)

    # Load cache and data.
    vivino_cache = load_cache(args.cache_file)
    comparison_rows = read_csv_rows(args.comparison)
    vivino_rows = read_csv_rows(args.vivino)
    override_rows = read_optional_csv_rows(args.vivino_overrides)
    lookup = build_vivino_lookup(vivino_rows + override_rows)

    # Determine which wines need resolving.
    wines_to_resolve: list[dict[str, str]] = []
    cache_hits = 0
    for row in comparison_rows:
        wine_name = (row.get("name_plat") or "").strip()
        if not wine_name:
            continue

        # Skip wines already matched via CSV (unless --all).
        if not args.all:
            _, method = match_vivino_row(wine_name, lookup)
            plat_rating = (row.get("platinum_vivino_rating") or "").strip()
            if method != "none" or plat_rating:
                continue

        # Skip wines with fresh cache entries (unless --force).
        ck = cache_key(wine_name)
        cached = vivino_cache.get(ck)
        if cached and not args.force and is_cache_fresh(cached, args.cache_ttl_days):
            cache_hits += 1
            continue

        wines_to_resolve.append(row)

    if args.limit > 0:
        wines_to_resolve = wines_to_resolve[:args.limit]

    total = len(wines_to_resolve)
    print(
        f"[llm_resolver] {total} wines to resolve, {cache_hits} cached "
        f"(from {len(comparison_rows)} total, ttl={args.cache_ttl_days}d)"
    )

    resolved: list[dict[str, str]] = []
    for i, row in enumerate(wines_to_resolve, 1):
        wine_name = row["name_plat"].strip()
        print(f"[{i}/{total}] {wine_name[:65]}...")

        try:
            result = resolve_wine(
                wine_name,
                args.gemini_api_key,
                brave_api_key=args.brave_api_key,
                dry_run=args.dry_run,
                sleep_seconds=args.sleep,
            )
            status = (
                "ok" if result.get("vivino_rating")
                else "url_only" if result.get("vivino_url")
                else "failed"
            )
            print(
                f"  -> {status}: rating={result.get('vivino_rating') or 'N/A'}, "
                f"url={result.get('vivino_url', 'N/A')[:60]}"
            )
            resolved.append(result)

            # Update cache.
            if not args.dry_run:
                ck = cache_key(wine_name)
                vivino_cache[ck] = {
                    "wine_name": wine_name,
                    "vivino_url": result.get("vivino_url", ""),
                    "vivino_rating": result.get("vivino_rating", ""),
                    "vivino_num_ratings": result.get("vivino_num_ratings", ""),
                    "resolved_at": int(time.time()),
                    "notes": result.get("notes", ""),
                }
        except Exception as exc:
            print(f"  -> error: {exc}")
            resolved.append({"match_name": wine_name, "notes": f"error: {exc}"})

    # Also emit cached results for auto-apply (so overrides get full coverage).
    cached_for_apply: list[dict[str, str]] = []
    if args.auto_apply:
        for row in comparison_rows:
            wine_name = (row.get("name_plat") or "").strip()
            if not wine_name:
                continue
            ck = cache_key(wine_name)
            cached = vivino_cache.get(ck)
            if cached and cached.get("vivino_url"):
                cached_for_apply.append({
                    "match_name": cached.get("wine_name", wine_name),
                    "wine_name": cached.get("wine_name", ""),
                    "vivino_rating": cached.get("vivino_rating", ""),
                    "vivino_num_ratings": cached.get("vivino_num_ratings", ""),
                    "vivino_price": "",
                    "vivino_url": cached.get("vivino_url", ""),
                    "notes": f"cached resolved_at={cached.get('resolved_at', '')}",
                })

    # Save outputs.
    write_csv(args.output, resolved, _OVERRIDE_FIELDS)
    if not args.dry_run:
        save_cache(args.cache_file, vivino_cache)

    with_rating = sum(1 for r in resolved if (r.get("vivino_rating") or "").strip())
    with_url = sum(1 for r in resolved if (r.get("vivino_url") or "").strip())
    print(f"\n[llm_resolver] Wrote {len(resolved)} results to {args.output}")
    print(f"[llm_resolver] With rating: {with_rating}/{len(resolved)}, With URL: {with_url}/{len(resolved)}")
    print(f"[llm_resolver] Cache: {len(vivino_cache)} entries in {args.cache_file}")

    if args.auto_apply:
        all_apply = resolved + cached_for_apply
        merged = upsert_overrides(override_rows, all_apply)
        write_csv(args.vivino_overrides, merged, _OVERRIDE_FIELDS)
        print(f"[llm_resolver] Auto-applied: {len(merged)} total overrides in {args.vivino_overrides}")


if __name__ == "__main__":
    main()
