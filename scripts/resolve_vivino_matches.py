import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote_plus, urlencode, urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.import_wine_data import (  # noqa: E402
    build_vivino_lookup,
    canonicalize_key,
    extract_year,
    match_vivino_row,
    normalize_key,
    read_csv_rows,
    read_optional_csv_rows,
)
from scripts.vivino_fetch import (  # noqa: E402
    FACTS_KEY,
    PoliteFetcher,
    VivinoBlocked,
    canonical_wine_url,
    fetch_facts,
)
from scripts.vivino_overrides import OVERRIDE_FIELDS, upsert_overrides  # noqa: E402

RESOLVER_STATE_KEY = "vivino_resolver_state"
QUERY_CACHE_KEY = "vivino_query_cache"

_REVIEW_FIELDS = [
    "wine_name",
    "year",
    "producer",
    "label",
    "color",
    "query_1",
    "query_2",
    "query_3",
    "vivino_search_url",
    "candidate_count",
    "best_score",
    "second_score",
    "best_title",
    "best_url",
    "best_provider",
    "best_query",
    "decision",
    "reason",
]

_UNMATCHED_FIELDS = [
    "wine_name",
    "year",
    "producer",
    "label",
    "platinum_url",
    "grand_cru_url",
    "query_1",
    "query_2",
    "query_3",
    "vivino_search_url",
    "best_score",
    "best_url",
    "best_provider",
    "decision",
    "reason",
]

_COLOR_TOKENS = {"red", "white", "rose", "sparkling", "orange"}


@dataclass
class WineIdentity:
    wine_name: str
    year: int | None
    producer: str
    label: str
    color: str
    producer_tokens: set[str]
    target_tokens: set[str]


@dataclass
class Candidate:
    url: str
    title: str
    query: str
    provider: str
    score: float
    producer_overlap: int
    year_match: bool


def vivino_row_has_metrics(row: dict[str, str] | None) -> bool:
    if not row:
        return False
    return bool(
        (row.get("vivino_rating") or "").strip()
        or (row.get("vivino_num_ratings") or "").strip()
        or (row.get("vivino_raters") or "").strip()
    )


def load_resolver_state() -> dict[str, object]:
    """Resolver memory lives in the database; the container disk is wiped on every deploy."""
    from app.durable_state import load_state

    payload = load_state(RESOLVER_STATE_KEY, {})
    if not isinstance(payload, dict):
        payload = {}
    if not isinstance(payload.get("seen_unresolved"), dict):
        payload["seen_unresolved"] = {}
    return payload


def save_resolver_state(state: dict[str, object]) -> None:
    from app.durable_state import save_state

    save_state(RESOLVER_STATE_KEY, state)


# Days to wait before searching an unmatched wine again, by number of tries so far.
RETRY_AFTER_DAYS = (1, 3, 7, 30)


def due_for_search(fingerprint: str, state: dict[str, object], now: float) -> bool:
    """New wines are due at once; unmatched ones come back after 1, 3, 7, then every 30 days."""
    seen = state.get("seen_unresolved") or {}
    if fingerprint not in seen:
        return True
    tries = int((state.get("search_attempts") or {}).get(fingerprint, 1))
    wait = RETRY_AFTER_DAYS[min(tries, len(RETRY_AFTER_DAYS)) - 1] * 86400
    return now - float(seen[fingerprint]) >= wait


def search_priority(row: dict[str, str]) -> tuple[int, float]:
    """In-stock wines first, dearest first: that is where a market price matters most."""
    sold_out = (row.get("platinum_in_stock") or "").strip().lower() == "false"
    try:
        price = float(str(row.get("price_plat") or "0").replace(",", "").replace("$", ""))
    except ValueError:
        price = 0.0
    return (1 if sold_out else 0, -price)


def unresolved_fingerprint(row: dict[str, str]) -> str:
    name = (row.get("name_plat") or "").strip()
    year = (row.get("year_plat") or "").strip()
    url_main = (row.get("url_main") or "").strip()
    url_plat = (row.get("url_plat") or "").strip()
    raw = f"{name}|{year}|{url_main}|{url_plat}"
    return canonicalize_key(raw)


def write_csv_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _safe_slug_text(url: str) -> str:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if "w" in parts:
        idx = parts.index("w")
        if idx > 0:
            return parts[idx - 1].replace("-", " ")
    if parts:
        return parts[-1].replace("-", " ")
    return ""


def _product_slug_text(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return ""
    slug = parts[-1].split("?")[0]
    return slug.replace("-", " ").strip()


def parse_identity(row: dict[str, str]) -> WineIdentity:
    wine_name = (row.get("name_plat") or "").strip()
    parts = [part.strip() for part in wine_name.split(" - ") if part.strip()]

    primary = parts[0] if parts else wine_name
    year = extract_year(row.get("year_plat")) or extract_year(wine_name)

    producer = re.sub(r"^(?:19|20)\d{2}\s+", "", primary, flags=re.IGNORECASE)
    producer = re.sub(r"^nv\s+", "", producer, flags=re.IGNORECASE).strip()
    producer = producer or primary.strip()

    label = parts[1] if len(parts) > 1 else producer
    color = normalize_key(parts[2]) if len(parts) > 2 else ""
    if color not in _COLOR_TOKENS:
        color = ""

    target_text = " ".join(piece for piece in [str(year) if year else "", producer, label] if piece)
    target_tokens = set(canonicalize_key(target_text).split())
    producer_tokens = {token for token in canonicalize_key(producer).split() if len(token) >= 3}

    return WineIdentity(
        wine_name=wine_name,
        year=year,
        producer=producer,
        label=label,
        color=color,
        producer_tokens=producer_tokens,
        target_tokens=target_tokens,
    )


def build_queries(identity: WineIdentity, row: dict[str, str]) -> list[str]:
    query_terms = [
        " ".join(
            part
            for part in [str(identity.year) if identity.year else "", identity.producer, identity.label, "site:vivino.com"]
            if part
        ),
        " ".join(part for part in [identity.producer, identity.label, identity.color, "site:vivino.com"] if part),
    ]

    slug_hint = _product_slug_text((row.get("url_main") or "").strip())
    if not slug_hint:
        slug_hint = _product_slug_text((row.get("url_plat") or "").strip())
    if slug_hint:
        query_terms.append(f"{slug_hint} site:vivino.com")

    deduped: list[str] = []
    seen: set[str] = set()
    for query in query_terms:
        cleaned = re.sub(r"\s+", " ", query).strip()
        if cleaned and cleaned not in seen:
            deduped.append(cleaned)
            seen.add(cleaned)

    return deduped[:3]


def build_vivino_search_url(identity: WineIdentity) -> str:
    base_query = " ".join(
        part for part in [str(identity.year) if identity.year else "", identity.producer, identity.label] if part
    )
    return f"https://www.vivino.com/en/search/wines?q={quote_plus(base_query)}"


def normalize_vivino_url(url: str | None) -> str:
    """Canonical wine URL, so /en/ and /US/en/ links to one wine count as one candidate."""
    return canonical_wine_url(url) or ""


def search_serper(query: str, api_key: str, max_results: int) -> list[dict[str, str]]:
    request_body = json.dumps({"q": query, "num": max_results}).encode("utf-8")
    request = Request(
        "https://google.serper.dev/search",
        data=request_body,
        method="POST",
        headers={
            "X-API-KEY": api_key,
            "Content-Type": "application/json",
        },
    )

    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    results: list[dict[str, str]] = []
    for item in payload.get("organic", []) or []:
        link = (item.get("link") or "").strip()
        title = (item.get("title") or "").strip()
        if link:
            results.append({"url": link, "title": title})
    return results


def search_google_cse(query: str, api_key: str, cse_id: str, max_results: int) -> list[dict[str, str]]:
    if max_results <= 0:
        return []

    params = {
        "key": api_key,
        "cx": cse_id,
        "q": query,
        "num": max(1, min(max_results, 10)),
    }
    url = f"https://customsearch.googleapis.com/customsearch/v1?{urlencode(params)}"
    request = Request(url, method="GET")

    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    results: list[dict[str, str]] = []
    for item in payload.get("items", []) or []:
        link = (item.get("link") or "").strip()
        title = (item.get("title") or "").strip()
        if link:
            results.append({"url": link, "title": title})
    return results


def search_brave(query: str, api_key: str, max_results: int) -> list[dict[str, str]]:
    if max_results <= 0:
        return []

    params = {
        "q": query,
        "count": max(1, min(max_results, 20)),
    }
    url = f"https://api.search.brave.com/res/v1/web/search?{urlencode(params)}"
    request = Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "X-Subscription-Token": api_key,
        },
    )

    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    results: list[dict[str, str]] = []
    web = payload.get("web") or {}
    for item in web.get("results", []) or []:
        link = (item.get("url") or "").strip()
        title = (item.get("title") or "").strip()
        if link:
            results.append({"url": link, "title": title})
    return results


def run_search(
    provider: str,
    query: str,
    max_results: int,
    serper_api_key: str,
    google_api_key: str,
    google_cse_id: str,
    brave_api_key: str,
) -> list[dict[str, str]]:
    if provider == "none":
        return []
    if provider == "serper":
        if not serper_api_key:
            raise ValueError("SERPER_API_KEY is required for --provider serper")
        return search_serper(query=query, api_key=serper_api_key, max_results=max_results)
    if provider == "google_cse":
        if not google_api_key:
            raise ValueError("GOOGLE_API_KEY is required for --provider google_cse")
        if not google_cse_id:
            raise ValueError("GOOGLE_CSE_ID is required for --provider google_cse")
        return search_google_cse(
            query=query,
            api_key=google_api_key,
            cse_id=google_cse_id,
            max_results=max_results,
        )
    if provider == "brave":
        if not brave_api_key:
            raise ValueError("BRAVE_API_KEY is required for --provider brave")
        return search_brave(query=query, api_key=brave_api_key, max_results=max_results)
    raise ValueError(f"Unsupported provider: {provider}")


def load_query_cache() -> dict[str, dict[str, object]]:
    from app.durable_state import load_state

    payload = load_state(QUERY_CACHE_KEY, {})
    if not isinstance(payload, dict):
        return {}
    return {key: value for key, value in payload.items() if isinstance(key, str) and isinstance(value, dict)}


def save_query_cache(cache: dict[str, dict[str, object]], ttl_hours: float) -> None:
    from app.durable_state import save_state

    if ttl_hours > 0:
        cutoff = time.time() - ttl_hours * 3600
        cache = {k: v for k, v in cache.items() if float(v.get("timestamp") or 0) >= cutoff}
    save_state(QUERY_CACHE_KEY, cache)


def _build_query_cache_key(provider: str, query: str, max_results: int) -> str:
    cleaned_query = re.sub(r"\s+", " ", query.strip().lower())
    return f"{provider}|{max_results}|{cleaned_query}"


def _read_cache_results(
    cache: dict[str, dict[str, object]],
    cache_key: str,
    cache_ttl_hours: float,
) -> list[dict[str, str]] | None:
    entry = cache.get(cache_key)
    if not entry:
        return None

    timestamp = entry.get("timestamp")
    if isinstance(timestamp, (int, float)) and cache_ttl_hours > 0:
        age_seconds = time.time() - float(timestamp)
        if age_seconds > (cache_ttl_hours * 3600):
            return None

    results = entry.get("results")
    if not isinstance(results, list):
        return None

    normalized: list[dict[str, str]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        title = str(item.get("title") or "").strip()
        if url:
            normalized.append({"url": url, "title": title})
    return normalized


def _write_cache_results(
    cache: dict[str, dict[str, object]],
    cache_key: str,
    results: list[dict[str, str]],
) -> None:
    cache[cache_key] = {
        "timestamp": int(time.time()),
        "results": results,
    }


def _provider_has_credentials(
    provider: str,
    *,
    serper_api_key: str,
    google_api_key: str,
    google_cse_id: str,
    brave_api_key: str,
) -> bool:
    if provider == "none":
        return True
    if provider == "serper":
        return bool(serper_api_key)
    if provider == "google_cse":
        return bool(google_api_key and google_cse_id)
    if provider == "brave":
        return bool(brave_api_key)
    return False


def _resolve_provider_order(
    requested_provider: str,
    *,
    auto_provider_order: str,
    serper_api_key: str,
    google_api_key: str,
    google_cse_id: str,
    brave_api_key: str,
) -> list[str]:
    if requested_provider != "auto":
        return [requested_provider]

    configured = [part.strip().lower() for part in auto_provider_order.split(",") if part.strip()]
    if not configured:
        configured = ["google_cse", "brave", "serper"]

    valid = {"google_cse", "brave", "serper"}
    providers: list[str] = []
    for provider in configured:
        if provider not in valid:
            continue
        if provider in providers:
            continue
        if _provider_has_credentials(
            provider,
            serper_api_key=serper_api_key,
            google_api_key=google_api_key,
            google_cse_id=google_cse_id,
            brave_api_key=brave_api_key,
        ):
            providers.append(provider)

    if not providers:
        providers.append("none")
    return providers


def search_with_cache_and_fallback(
    *,
    requested_provider: str,
    query: str,
    max_results: int,
    serper_api_key: str,
    google_api_key: str,
    google_cse_id: str,
    brave_api_key: str,
    auto_provider_order: str,
    query_cache: dict[str, dict[str, object]],
    cache_ttl_hours: float,
    max_api_queries: int,
    api_calls_state: dict[str, int],
) -> tuple[list[dict[str, str]], str, bool, list[str]]:
    providers = _resolve_provider_order(
        requested_provider,
        auto_provider_order=auto_provider_order,
        serper_api_key=serper_api_key,
        google_api_key=google_api_key,
        google_cse_id=google_cse_id,
        brave_api_key=brave_api_key,
    )

    errors: list[str] = []
    had_cache_hit = False
    fallback_provider = providers[-1] if providers else requested_provider

    for provider in providers:
        if provider == "none":
            return ([], "none", had_cache_hit, errors)

        if not _provider_has_credentials(
            provider,
            serper_api_key=serper_api_key,
            google_api_key=google_api_key,
            google_cse_id=google_cse_id,
            brave_api_key=brave_api_key,
        ):
            errors.append(f"{provider}:missing_credentials")
            continue

        cache_key = _build_query_cache_key(provider, query, max_results)
        cached_results = _read_cache_results(query_cache, cache_key, cache_ttl_hours)
        if cached_results is not None:
            had_cache_hit = True
            if cached_results:
                return (cached_results, provider, True, errors)
            errors.append(f"{provider}:cache_empty_retrying_live")

        if max_api_queries > 0 and api_calls_state["count"] >= max_api_queries:
            errors.append(f"{provider}:max_api_queries_reached")
            continue

        api_calls_state["count"] += 1
        try:
            live_results = run_search(
                provider=provider,
                query=query,
                max_results=max_results,
                serper_api_key=serper_api_key,
                google_api_key=google_api_key,
                google_cse_id=google_cse_id,
                brave_api_key=brave_api_key,
            )
        except (HTTPError, URLError, ValueError) as exc:
            errors.append(f"{provider}:{exc}")
            continue

        _write_cache_results(query_cache, cache_key, live_results)
        if live_results:
            return (live_results, provider, False, errors)
        errors.append(f"{provider}:no_results")

    return ([], fallback_provider, had_cache_hit, errors)


def _token_set_ratio(target_tokens: set[str], candidate_tokens: set[str]) -> float:
    inter = sorted(target_tokens & candidate_tokens)
    if not inter:
        return 0.0

    inter_text = " ".join(inter)
    target_text = " ".join(sorted(target_tokens))
    candidate_text = " ".join(sorted(candidate_tokens))
    return max(
        SequenceMatcher(None, inter_text, target_text).ratio(),
        SequenceMatcher(None, inter_text, candidate_text).ratio(),
    )


def score_candidate(identity: WineIdentity, title: str, url: str) -> tuple[float, int, bool]:
    candidate_text = f"{title} {_safe_slug_text(url)}"
    candidate_key = canonicalize_key(candidate_text)
    candidate_tokens = set(candidate_key.split())
    if not identity.target_tokens or not candidate_tokens:
        return (0.0, 0, False)

    overlap = len(identity.target_tokens & candidate_tokens)
    if overlap == 0:
        return (0.0, 0, False)

    token_ratio = overlap / max(len(identity.target_tokens), len(candidate_tokens))
    seq_ratio = SequenceMatcher(
        None,
        " ".join(sorted(identity.target_tokens)),
        " ".join(sorted(candidate_tokens)),
    ).ratio()
    set_ratio = _token_set_ratio(identity.target_tokens, candidate_tokens)
    score = (token_ratio * 0.45) + (seq_ratio * 0.20) + (set_ratio * 0.35)

    producer_overlap = len(identity.producer_tokens & candidate_tokens) if identity.producer_tokens else 0
    if identity.producer_tokens and producer_overlap == 0:
        score -= 0.25
    elif producer_overlap > 0:
        score += min(0.08, producer_overlap * 0.03)

    year_match = False
    if identity.year is not None:
        query_year: int | None = None
        year_values = parse_qs(urlparse(url).query).get("year", [])
        if year_values:
            try:
                query_year = int(year_values[0])
            except ValueError:
                query_year = None
        if query_year is None:
            query_year = extract_year(candidate_text)

        if query_year == identity.year:
            score += 0.10
            year_match = True
        elif query_year is not None:
            score -= 0.10

    if identity.color:
        raw_tokens = set(normalize_key(candidate_text).split())
        if identity.color in raw_tokens:
            score += 0.03

    score = max(0.0, min(1.0, score))
    return (score, producer_overlap, year_match)


def rank_candidates(
    identity: WineIdentity,
    results_by_query: list[tuple[str, str, list[dict[str, str]]]],
) -> list[Candidate]:
    """Score search hits, one candidate per Vivino wine, best first."""
    candidates_by_url: dict[str, Candidate] = {}
    for query, provider, results in results_by_query:
        for result in results:
            normalized_url = normalize_vivino_url(result.get("url"))
            if not normalized_url:
                continue
            score, producer_overlap, year_match = score_candidate(
                identity=identity,
                title=result.get("title", ""),
                url=normalized_url,
            )
            if score <= 0:
                continue
            existing = candidates_by_url.get(normalized_url)
            if existing is None or score > existing.score:
                candidates_by_url[normalized_url] = Candidate(
                    url=normalized_url,
                    title=(result.get("title") or _safe_slug_text(normalized_url)).strip(),
                    query=query,
                    provider=provider,
                    score=score,
                    producer_overlap=producer_overlap,
                    year_match=year_match,
                )
    return sorted(candidates_by_url.values(), key=lambda c: c.score, reverse=True)


def decide(ranked: list[Candidate], args: argparse.Namespace) -> tuple[str, str]:
    best = ranked[0] if ranked else None
    margin = (best.score - ranked[1].score) if len(ranked) > 1 else (best.score if best else 0.0)
    review_threshold = max(args.min_confidence - 0.12, 0.55)
    if args.provider == "none":
        return "no_provider", "provider=none; generated deterministic queries only"
    if best is None:
        return "unmatched", "no viable vivino candidates returned"
    if args.auto_accept_best:
        return "auto_accept", (
            f"auto_accept_best enabled; score={best.score:.3f}, margin={margin:.3f}, "
            f"producer_overlap={best.producer_overlap}"
        )
    if best.producer_overlap == 0:
        return "needs_review", "top candidate missing producer token overlap"
    if best.score >= args.min_confidence and margin >= args.min_margin:
        return "auto_accept", f"score={best.score:.3f}, margin={margin:.3f}"
    if best.score >= review_threshold:
        return "needs_review", f"score={best.score:.3f}, margin={margin:.3f}"
    return "unmatched", f"score below threshold ({best.score:.3f} < {review_threshold:.3f})"


PAGE_NOT_CHECKED = "vivino page not checked yet"


def gate_on_page_metrics(
    identity: WineIdentity,
    decision: str,
    reason: str,
    page_facts: dict[str, object] | None,
    args: argparse.Namespace,
) -> tuple[str, str]:
    """Auto-apply only when the candidate's own Vivino page has a rating or count."""
    if decision != "auto_accept" or not args.require_vivino_metrics:
        return decision, reason
    if page_facts is None:
        return "needs_review", f"{reason}; {PAGE_NOT_CHECKED}"
    if page_facts.get("rating") is None and page_facts.get("num_ratings") is None:
        return "needs_review", f"{reason}; vivino page has no rating/count"
    page_tokens = set(canonicalize_key(str(page_facts.get("name") or "")).split())
    if page_tokens and identity.producer_tokens and not (identity.producer_tokens & page_tokens):
        return "needs_review", f"{reason}; vivino page name {page_facts.get('name')!r} lacks the producer"
    return decision, reason


def resolve_matches(args: argparse.Namespace) -> None:
    if args.provider == "brave" and not args.brave_api_key:
        raise ValueError("BRAVE_API_KEY is required for --provider brave")
    if args.provider == "serper" and not args.serper_api_key:
        raise ValueError("SERPER_API_KEY is required for --provider serper")
    if args.provider == "google_cse" and (not args.google_api_key or not args.google_cse_id):
        raise ValueError("GOOGLE_API_KEY and GOOGLE_CSE_ID are required for --provider google_cse")

    from scripts.llm_utils import load_identity_cache, needs_resolution

    comparison_rows = read_csv_rows(args.comparison)
    vivino_rows = read_csv_rows(args.vivino)
    from scripts.vivino_overrides import with_durable_overrides

    override_rows = with_durable_overrides(read_optional_csv_rows(args.vivino_overrides))
    state = load_resolver_state()
    seen_unresolved = state.get("seen_unresolved")
    if not isinstance(seen_unresolved, dict):
        seen_unresolved = {}
        state["seen_unresolved"] = seen_unresolved

    identity_cache = load_identity_cache()
    identity_skipped = 0

    initial_lookup = build_vivino_lookup(vivino_rows + override_rows)
    unresolved_rows: list[dict[str, str]] = []
    unresolved_none_count = 0
    missing_url_enrichment_count = 0
    missing_metrics_enrichment_count = 0
    for row in comparison_rows:
        wine_name = (row.get("name_plat") or "").strip()
        if not wine_name:
            continue

        # Skip wines with validated Vivino URL in identity cache
        if not needs_resolution(identity_cache, wine_name):
            identity_skipped += 1
            continue

        matched_row, match_method = match_vivino_row(wine_name, initial_lookup)
        if match_method == "none":
            unresolved_rows.append(row)
            unresolved_none_count += 1
            continue

        matched_url = normalize_vivino_url((matched_row or {}).get("vivino_url"))
        if not matched_url:
            unresolved_rows.append(row)
            missing_url_enrichment_count += 1
            continue

        if not vivino_row_has_metrics(matched_row):
            unresolved_rows.append(row)
            missing_metrics_enrichment_count += 1

    total_unresolved_before_filter = len(unresolved_rows)
    skipped_seen = 0
    attempts = state.setdefault("search_attempts", {})
    never_searched = sum(1 for row in unresolved_rows if unresolved_fingerprint(row) not in seen_unresolved)
    if args.only_new_unresolved:
        now_ts = time.time()
        filtered_rows: list[dict[str, str]] = []
        for row in unresolved_rows:
            if not due_for_search(unresolved_fingerprint(row), state, now_ts):
                skipped_seen += 1
                continue
            filtered_rows.append(row)
        unresolved_rows = filtered_rows
    unresolved_rows.sort(key=search_priority)

    if args.limit > 0:
        unresolved_rows = unresolved_rows[: args.limit]

    print(
        "[resolve] input:",
        f"comparison={len(comparison_rows)}",
        f"vivino={len(vivino_rows)}",
        f"overrides={len(override_rows)}",
        f"identity_cache_skipped={identity_skipped}",
        f"unresolved_before_filter={total_unresolved_before_filter}",
        f"unresolved_none={unresolved_none_count}",
        f"missing_url_enrichment={missing_url_enrichment_count}",
        f"missing_metrics_enrichment={missing_metrics_enrichment_count}",
        f"skipped_seen={skipped_seen}",
        f"unresolved={len(unresolved_rows)}",
    )

    query_cache = load_query_cache()
    fetcher: PoliteFetcher | None = None
    vivino_facts: dict[str, dict[str, object]] = {}
    if args.require_vivino_metrics and args.provider != "none":
        from app.durable_state import load_state as load_durable

        fetcher = PoliteFetcher()
        vivino_facts = load_durable(FACTS_KEY, {}) or {}
    api_calls_state = {"count": 0}
    cache_hits = 0
    provider_usage: dict[str, int] = {}

    review_rows: list[dict[str, str]] = []
    unmatched_rows: list[dict[str, str]] = []
    accepted_rows: list[dict[str, str]] = []

    searched = 0
    for index, row in enumerate(unresolved_rows, start=1):
        if api_calls_state["count"] >= args.max_api_queries > 0:
            break
        row_fingerprint = unresolved_fingerprint(row)
        seen_unresolved[row_fingerprint] = int(time.time())
        attempts[row_fingerprint] = int(attempts.get(row_fingerprint, 0)) + 1
        searched += 1

        identity = parse_identity(row)
        queries = build_queries(identity, row)
        vivino_search_url = build_vivino_search_url(identity)

        results_by_query: list[tuple[str, str, list[dict[str, str]]]] = []
        search_errors: list[str] = []

        for query in queries:
            results, provider_used, cache_hit, provider_errors = search_with_cache_and_fallback(
                requested_provider=args.provider,
                query=query,
                max_results=args.max_results,
                serper_api_key=args.serper_api_key,
                google_api_key=args.google_api_key,
                google_cse_id=args.google_cse_id,
                brave_api_key=args.brave_api_key,
                auto_provider_order=args.auto_provider_order,
                query_cache=query_cache,
                cache_ttl_hours=args.cache_ttl_hours,
                max_api_queries=args.max_api_queries,
                api_calls_state=api_calls_state,
            )

            provider_usage[provider_used] = provider_usage.get(provider_used, 0) + 1
            if cache_hit:
                cache_hits += 1
            if provider_errors:
                search_errors.extend(provider_errors)
            results_by_query.append((query, provider_used, results))

            if args.sleep_seconds > 0 and not cache_hit:
                time.sleep(args.sleep_seconds)
            # A confident match from the first query saves the other queries for other wines.
            if decide(rank_candidates(identity, results_by_query), args)[0] == "auto_accept":
                break

        ranked = rank_candidates(identity, results_by_query)
        best = ranked[0] if ranked else None
        second = ranked[1] if len(ranked) > 1 else None
        best_score = best.score if best else 0.0
        second_score = second.score if second else 0.0
        margin = best_score - second_score

        decision, reason = decide(ranked, args)
        if search_errors:
            reason = f"{reason}; search_error={search_errors[0]}"

        page_facts: dict[str, object] | None = None
        if decision == "auto_accept" and best is not None and fetcher is not None:
            try:
                page_facts = fetch_facts(fetcher, vivino_facts, best.url, identity.year)
            except VivinoBlocked as exc:
                print(f"[resolve] Vivino refused a page ({exc}); no more page checks this run.")
        decision, reason = gate_on_page_metrics(identity, decision, reason, page_facts, args)

        # Retry tomorrow instead of forgetting: API cap hit or page not checked yet.
        capped = not ranked and any("max_api_queries_reached" in err for err in search_errors)
        if capped or PAGE_NOT_CHECKED in reason:
            seen_unresolved.pop(row_fingerprint, None)
            attempts.pop(row_fingerprint, None)
            searched -= 1
        if decision == "auto_accept":
            attempts.pop(row_fingerprint, None)

        query_1 = queries[0] if len(queries) > 0 else ""
        query_2 = queries[1] if len(queries) > 1 else ""
        query_3 = queries[2] if len(queries) > 2 else ""

        review_rows.append(
            {
                "wine_name": identity.wine_name,
                "year": str(identity.year or ""),
                "producer": identity.producer,
                "label": identity.label,
                "color": identity.color,
                "query_1": query_1,
                "query_2": query_2,
                "query_3": query_3,
                "vivino_search_url": vivino_search_url,
                "candidate_count": str(len(ranked)),
                "best_score": f"{best_score:.4f}" if best else "",
                "second_score": f"{second_score:.4f}" if second else "",
                "best_title": best.title if best else "",
                "best_url": best.url if best else "",
                "best_provider": best.provider if best else "",
                "best_query": best.query if best else "",
                "decision": decision,
                "reason": reason,
            }
        )

        if decision == "auto_accept" and best is not None:
            page = page_facts or {}
            accepted_rows.append(
                {
                    "match_name": identity.wine_name,
                    "wine_name": str(page.get("name") or best.title),
                    "vivino_rating": "" if page.get("rating") is None else str(page["rating"]),
                    "vivino_num_ratings": "" if page.get("num_ratings") is None else str(page["num_ratings"]),
                    "vivino_price": "",
                    "vivino_url": best.url,
                    "notes": (
                        f"auto_resolved provider={best.provider or args.provider} "
                        f"score={best.score:.3f} margin={margin:.3f}"
                    ),
                }
            )
        else:
            unmatched_rows.append(
                {
                    "wine_name": identity.wine_name,
                    "year": str(identity.year or ""),
                    "producer": identity.producer,
                    "label": identity.label,
                    "platinum_url": (row.get("url_plat") or "").strip(),
                    "grand_cru_url": (row.get("url_main") or "").strip(),
                    "query_1": query_1,
                    "query_2": query_2,
                    "query_3": query_3,
                    "vivino_search_url": vivino_search_url,
                    "best_score": f"{best_score:.4f}" if best else "",
                    "best_url": best.url if best else "",
                    "best_provider": best.provider if best else "",
                    "decision": decision,
                    "reason": reason,
                }
            )

        print(
            f"[resolve] {index}/{len(unresolved_rows)}",
            identity.wine_name,
            f"-> {decision}",
            f"best={best_score:.3f}",
            f"candidates={len(ranked)}",
        )

    write_csv_rows(args.output_review, review_rows, _REVIEW_FIELDS)
    write_csv_rows(args.output_unmatched, unmatched_rows, _UNMATCHED_FIELDS)
    write_csv_rows(args.output_suggestions, accepted_rows, OVERRIDE_FIELDS)

    applied_overrides_rows = len(override_rows)
    if args.auto_apply and accepted_rows:
        merged = upsert_overrides(override_rows, accepted_rows)
        write_csv_rows(args.vivino_overrides, merged, OVERRIDE_FIELDS)
        applied_overrides_rows = len(merged)

    save_query_cache(query_cache, args.cache_ttl_hours)
    state["last_run_at"] = int(time.time())
    state["last_run"] = {
        "at": datetime.now(UTC).isoformat(),
        "searched": searched,
        "resolved": len(accepted_rows),
        "api_calls": api_calls_state["count"],
        "cache_hits": cache_hits,
        "unresolved": total_unresolved_before_filter - len(accepted_rows),
        "never_searched": max(never_searched - searched, 0),
        "waiting_to_retry": skipped_seen,
    }
    save_resolver_state(state)
    if accepted_rows:
        from scripts.vivino_overrides import save_durable_overrides

        save_durable_overrides(accepted_rows)
    if fetcher is not None:
        from app.durable_state import save_state as save_durable

        save_durable(FACTS_KEY, vivino_facts)
        print("[resolve] vivino page checks:", fetcher.finish())

    print(
        "[resolve] summary:",
        f"review_rows={len(review_rows)}",
        f"auto_accepted={len(accepted_rows)}",
        f"unmatched_or_review={len(unmatched_rows)}",
        f"overrides_rows={applied_overrides_rows}",
        f"api_calls={api_calls_state['count']}",
        f"cache_hits={cache_hits}",
        f"providers={provider_usage}",
        f"review_output={args.output_review}",
        f"unmatched_output={args.output_unmatched}",
        f"suggestions_output={args.output_suggestions}",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve unmatched Platinum wines to Vivino links using deterministic queries and confidence scoring."
    )
    parser.add_argument("--comparison", type=Path, default=Path("seed/comparison_summary.csv"))
    parser.add_argument("--vivino", type=Path, default=Path("seed/vivino_results.csv"))
    parser.add_argument("--vivino-overrides", type=Path, default=Path("seed/vivino_overrides.csv"))
    parser.add_argument("--provider", choices=["none", "auto", "serper", "google_cse", "brave"], default="none")
    parser.add_argument("--serper-api-key", default=os.getenv("SERPER_API_KEY", ""))
    parser.add_argument(
        "--google-api-key",
        default=os.getenv("GOOGLE_API_KEY", os.getenv("GOOGLE_CSE_API_KEY", "")),
    )
    parser.add_argument("--google-cse-id", default=os.getenv("GOOGLE_CSE_ID", ""))
    parser.add_argument("--brave-api-key", default=os.getenv("BRAVE_API_KEY", ""))
    parser.add_argument(
        "--auto-provider-order",
        default=os.getenv("VIVINO_AUTO_PROVIDER_ORDER", "google_cse,brave,serper"),
        help="Comma-separated provider order used when --provider auto.",
    )
    parser.add_argument(
        "--cache-ttl-hours",
        type=float,
        default=168.0,
        help="Cache freshness window in hours (default 168 = 7 days).",
    )
    parser.add_argument(
        "--max-api-queries",
        type=int,
        default=0,
        help="Hard cap on external search API calls per run (0 = unlimited).",
    )
    parser.add_argument(
        "--only-new-unresolved",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only query unresolved rows not seen before (state kept in the database; default: true).",
    )
    parser.add_argument("--max-results", type=int, default=8)
    parser.add_argument("--sleep-seconds", type=float, default=1.2)
    parser.add_argument("--min-confidence", type=float, default=0.82)
    parser.add_argument("--min-margin", type=float, default=0.08)
    parser.add_argument(
        "--auto-accept-best",
        action="store_true",
        help="Auto-accept the best candidate even if below confidence thresholds.",
    )
    parser.add_argument(
        "--require-vivino-metrics",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Auto-apply only when the candidate's Vivino page shows a rating/count (default: true).",
    )
    # Deprecated alias — kept so old ad-hoc invocations surface a clear error.
    parser.add_argument(
        "--allow-missing-vivino-metrics",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--limit", type=int, default=0, help="Optional max unresolved wines to process (0 means all).")
    parser.add_argument("--auto-apply", action="store_true", help="Append auto-accepted rows into --vivino-overrides")
    parser.add_argument("--output-review", type=Path, default=Path("data/vivino_review_queue.csv"))
    parser.add_argument("--output-unmatched", type=Path, default=Path("data/vivino_unmatched.csv"))
    parser.add_argument("--output-suggestions", type=Path, default=Path("data/vivino_auto_overrides.csv"))
    args = parser.parse_args()

    if getattr(args, "allow_missing_vivino_metrics", False):
        print(
            "[resolve] WARNING: --allow-missing-vivino-metrics is deprecated and has no effect. "
            "Vivino rating/count metrics are now required for auto-apply by default. "
            "Use --no-require-vivino-metrics only for an intentional one-off review run.",
            file=sys.stderr,
        )

    resolve_matches(args)


if __name__ == "__main__":
    main()
