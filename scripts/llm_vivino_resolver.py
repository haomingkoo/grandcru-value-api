"""LLM-powered Vivino resolver using Gemini Flash.

For each unmatched wine, asks Gemini to extract structured identity and
generate the best Vivino search query. Brave finds the Vivino page; the
polite fetcher (scripts/vivino_fetch.py) reads its rating, rating count and
price within the shared daily budget.

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
from difflib import SequenceMatcher
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
from scripts.llm_utils import (  # noqa: E402
    call_gemini,
    cache_key,
    is_cache_fresh,
    load_cache,
    save_cache,
)
from scripts.vivino_fetch import (  # noqa: E402
    FACTS_KEY,
    PoliteFetcher,
    VivinoBlocked,
    canonical_wine_url,
    fetch_facts,
)
from scripts.vivino_overrides import OVERRIDE_FIELDS, upsert_overrides  # noqa: E402

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("grandcru.llm_resolver")

CACHE_TTL_DAYS = 30

_SIGNATURE_MARKERS = (
    "chardonnay",
    "pinot noir",
    "cabernet sauvignon",
    "sauvignon blanc",
    "pinot grigio",
    "pinot gris",
    "barolo",
    "barbera",
    "nebbiolo",
    "shiraz",
    "syrah",
    "primitivo",
    "pecorino",
    "rose",
    "brut",
    "champagne",
)


def vintage_hint(wine_name: str) -> str:
    match = re.match(r"^\s*((?:19|20)\d{2})\b", wine_name or "")
    return match.group(1) if match else ""


def _normalize_validation_name(value: str | None) -> str:
    text = re.sub(r"^\s*(?:19|20)\d{2}\s+", "", value or "", flags=re.IGNORECASE)
    text = re.sub(r"^\s*nv\s+", "", text, flags=re.IGNORECASE)
    normalized = canonicalize_key(text)
    return " ".join(token for token in normalized.split() if token not in {"base"})


def _extract_signature_tokens(text: str | None) -> set[str]:
    normalized = _normalize_validation_name(text)
    signatures: set[str] = set()
    for marker in _SIGNATURE_MARKERS:
        marker_key = canonicalize_key(marker)
        if marker_key and marker_key in normalized:
            signatures.add(marker_key)
    return signatures


def is_confident_vivino_page_match(
    *,
    expected_name: str,
    candidate_name: str,
    candidate_context: str = "",
) -> bool:
    expected = _normalize_validation_name(expected_name)
    candidate = _normalize_validation_name(candidate_name)
    if not expected or not candidate:
        return True

    expected_tokens = set(expected.split())
    candidate_tokens = set(candidate.split())
    overlap = expected_tokens & candidate_tokens
    if len(overlap) < 2:
        return False

    token_ratio = len(overlap) / max(len(expected_tokens), len(candidate_tokens))
    seq_ratio = SequenceMatcher(None, expected, candidate).ratio()
    if seq_ratio < 0.72 and token_ratio < 0.70:
        return False

    expected_signatures = _extract_signature_tokens(expected_name)
    candidate_signatures = _extract_signature_tokens(" ".join(part for part in [candidate_name, candidate_context] if part))
    if expected_signatures and candidate_signatures and not (expected_signatures & candidate_signatures):
        return False

    return True


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
            canonical = canonical_wine_url(link)
            if canonical:
                return canonical

    return None


def search_vivino_for_url(query: str, brave_api_key: str = "") -> str | None:
    """Find a Vivino wine URL through Brave; Vivino's own search pages are not fetched."""
    return search_vivino_via_brave(query, brave_api_key) if brave_api_key else None


# ── Resolution ─────────────────────────────────────────────────────────


def resolve_wine(
    wine_name: str,
    api_key: str,
    *,
    brave_api_key: str = "",
    dry_run: bool = False,
    sleep_seconds: float = 2.0,
    known_vivino_url: str = "",
    fetcher: PoliteFetcher | None = None,
    vivino_facts: dict | None = None,
) -> dict:
    """Resolve a single wine: LLM parse -> Vivino search -> fetch metrics.

    If known_vivino_url is provided (from identity cache), skips the
    Brave search and goes straight to fetching data from the known URL.
    Saves 1-2 Brave API calls per wine.
    """
    result: dict[str, str] = {f: "" for f in OVERRIDE_FIELDS}
    result["match_name"] = wine_name

    vivino_url = ""

    if known_vivino_url:
        # Identity cache hit — skip LLM parsing and Brave search entirely
        vivino_url = known_vivino_url
        result["vivino_url"] = vivino_url
        result["notes"] = "identity_cache_hit"
        logger.info("  Using cached Vivino URL: %s", vivino_url[:60])
    else:
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
            # Fall through to grounding fallback below.

        if vivino_url:
            result["vivino_url"] = vivino_url

    # Step 3: read rating, count and price from the wine page, politely.
    if vivino_url and fetcher is not None:
        try:
            facts = fetch_facts(fetcher, vivino_facts, vivino_url, vintage_hint(wine_name))
        except VivinoBlocked as exc:
            result["notes"] += f" vivino_blocked={exc}"
            facts = None
        if facts is None:
            result["notes"] += " page_not_checked"
            return result
        page_name = str(facts.get("name") or "")
        expected_name = result.get("wine_name") or wine_name
        if page_name and not is_confident_vivino_page_match(
            expected_name=expected_name,
            candidate_name=page_name,
        ):
            result["notes"] += f" identity_mismatch={page_name[:120]}"
            result["vivino_url"] = ""
            return result
        result["vivino_rating"] = "" if facts.get("rating") is None else str(facts["rating"])
        result["vivino_num_ratings"] = "" if facts.get("num_ratings") is None else str(facts["num_ratings"])

    return result


# ── CSV helpers ────────────────────────────────────────────────────────


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


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

    from app.durable_state import load_state, save_state
    from scripts.llm_utils import get_identity, load_identity_cache

    # Load cache and data.
    vivino_cache = load_cache(args.cache_file)
    identity_cache = load_identity_cache()
    comparison_rows = read_csv_rows(args.comparison)
    vivino_rows = read_csv_rows(args.vivino)
    override_rows = read_optional_csv_rows(args.vivino_overrides)
    lookup = build_vivino_lookup(vivino_rows + override_rows)

    # Determine which wines need resolving.
    # Wines with a validated identity cache entry get their known URL
    # passed through — skipping the Brave search step entirely.
    wines_to_resolve: list[tuple[dict[str, str], str]] = []  # (row, known_url)
    cache_hits = 0
    identity_hits = 0
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

        # Check identity cache for known Vivino URL (skip Brave search)
        identity = get_identity(identity_cache, wine_name)
        known_url = ""
        if identity and identity.get("vivino_url"):
            known_url = identity["vivino_url"]
            identity_hits += 1

        wines_to_resolve.append((row, known_url))

    if args.limit > 0:
        wines_to_resolve = wines_to_resolve[:args.limit]

    total = len(wines_to_resolve)
    brave_needed = sum(1 for _, url in wines_to_resolve if not url)
    print(
        f"[llm_resolver] {total} wines to resolve, {cache_hits} cached, "
        f"{identity_hits} identity-cached (skip Brave), {brave_needed} need Brave "
        f"(from {len(comparison_rows)} total, ttl={args.cache_ttl_days}d)"
    )

    fetcher = None if args.dry_run else PoliteFetcher()
    vivino_facts: dict = {} if fetcher is None else (load_state(FACTS_KEY, {}) or {})

    resolved: list[dict[str, str]] = []
    for i, (row, known_url) in enumerate(wines_to_resolve, 1):
        wine_name = row["name_plat"].strip()
        label = "CACHED-URL" if known_url else "BRAVE"
        print(f"[{i}/{total}] [{label}] {wine_name[:60]}...")

        try:
            result = resolve_wine(
                wine_name,
                args.gemini_api_key,
                brave_api_key=args.brave_api_key,
                dry_run=args.dry_run,
                known_vivino_url=known_url,
                sleep_seconds=args.sleep,
                fetcher=fetcher,
                vivino_facts=vivino_facts,
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
                    "vivino_url": cached.get("vivino_url", ""),
                    "notes": f"cached resolved_at={cached.get('resolved_at', '')}",
                })

    if fetcher is not None:
        save_state(FACTS_KEY, vivino_facts)
        print(f"[llm_resolver] vivino page checks: {fetcher.finish()}")

    # Save outputs.
    write_csv(args.output, resolved, OVERRIDE_FIELDS)
    if not args.dry_run:
        save_cache(args.cache_file, vivino_cache)

    with_rating = sum(1 for r in resolved if (r.get("vivino_rating") or "").strip())
    with_url = sum(1 for r in resolved if (r.get("vivino_url") or "").strip())
    print(f"\n[llm_resolver] Wrote {len(resolved)} results to {args.output}")
    print(f"[llm_resolver] With rating: {with_rating}/{len(resolved)}, With URL: {with_url}/{len(resolved)}")
    print(f"[llm_resolver] Cache: {len(vivino_cache)} entries in {args.cache_file}")

    if args.auto_apply:
        all_apply = [
            row
            for row in (resolved + cached_for_apply)
            if (row.get("vivino_rating") or "").strip() or (row.get("vivino_num_ratings") or "").strip()
        ]
        merged = upsert_overrides(override_rows, all_apply)
        write_csv(args.vivino_overrides, merged, OVERRIDE_FIELDS)
        print(f"[llm_resolver] Auto-applied: {len(merged)} total overrides in {args.vivino_overrides}")


if __name__ == "__main__":
    main()
