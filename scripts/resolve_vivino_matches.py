import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote_plus, urlencode, urlparse, urlunparse
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

_OVERRIDE_FIELDS = [
    "match_name",
    "wine_name",
    "vivino_rating",
    "vivino_num_ratings",
    "vivino_price",
    "vivino_url",
    "notes",
]

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


def load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"seen_unresolved": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"seen_unresolved": {}}
    if not isinstance(payload, dict):
        return {"seen_unresolved": {}}
    if not isinstance(payload.get("seen_unresolved"), dict):
        payload["seen_unresolved"] = {}
    return payload


def save_state(path: Path, state: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


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


def upsert_overrides(existing: list[dict[str, str]], new_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_name: dict[str, dict[str, str]] = {}
    for row in existing:
        key = (row.get("match_name") or "").strip()
        if key:
            by_name[key] = row.copy()

    for row in new_rows:
        key = (row.get("match_name") or "").strip()
        if not key:
            continue
        prior = by_name.get(key)
        if prior is None:
            by_name[key] = row.copy()
            continue

        merged = prior.copy()
        for field in _OVERRIDE_FIELDS:
            incoming = (row.get(field) or "").strip()
            if incoming:
                merged[field] = incoming
        merged["match_name"] = key
        by_name[key] = merged

    merged = list(by_name.values())
    merged.sort(key=lambda r: (r.get("match_name") or ""))
    return merged


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
    if not url:
        return ""
    parsed = urlparse(url.strip())
    if "vivino.com" not in (parsed.netloc or ""):
        return ""
    if "/w/" not in parsed.path:
        return ""

    query_map = parse_qs(parsed.query)
    keep: dict[str, str] = {}
    for key in ("year", "price_id", "ref"):
        values = query_map.get(key)
        if values:
            keep[key] = values[0]

    normalized_query = urlencode(keep)
    normalized_path = parsed.path.rstrip("/")
    return urlunparse((parsed.scheme or "https", parsed.netloc, normalized_path, "", normalized_query, ""))


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


def load_query_cache(path: Path) -> dict[str, dict[str, object]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict[str, object]] = {}
    for key, value in payload.items():
        if isinstance(key, str) and isinstance(value, dict):
            normalized[key] = value
    return normalized


def save_query_cache(path: Path, cache: dict[str, dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


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


def resolve_matches(args: argparse.Namespace) -> None:
    if args.provider == "brave" and not args.brave_api_key:
        raise ValueError("BRAVE_API_KEY is required for --provider brave")
    if args.provider == "serper" and not args.serper_api_key:
        raise ValueError("SERPER_API_KEY is required for --provider serper")
    if args.provider == "google_cse" and (not args.google_api_key or not args.google_cse_id):
        raise ValueError("GOOGLE_API_KEY and GOOGLE_CSE_ID are required for --provider google_cse")

    comparison_rows = read_csv_rows(args.comparison)
    vivino_rows = read_csv_rows(args.vivino)
    override_rows = read_optional_csv_rows(args.vivino_overrides)
    state = load_state(args.state_file)
    seen_unresolved = state.get("seen_unresolved")
    if not isinstance(seen_unresolved, dict):
        seen_unresolved = {}
        state["seen_unresolved"] = seen_unresolved

    initial_lookup = build_vivino_lookup(vivino_rows + override_rows)
    unresolved_rows: list[dict[str, str]] = []
    unresolved_none_count = 0
    missing_url_enrichment_count = 0
    for row in comparison_rows:
        wine_name = (row.get("name_plat") or "").strip()
        if not wine_name:
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

    total_unresolved_before_filter = len(unresolved_rows)
    skipped_seen = 0
    if args.only_new_unresolved:
        filtered_rows: list[dict[str, str]] = []
        for row in unresolved_rows:
            fingerprint = unresolved_fingerprint(row)
            if fingerprint in seen_unresolved:
                skipped_seen += 1
                continue
            filtered_rows.append(row)
        unresolved_rows = filtered_rows

    if args.limit > 0:
        unresolved_rows = unresolved_rows[: args.limit]

    print(
        "[resolve] input:",
        f"comparison={len(comparison_rows)}",
        f"vivino={len(vivino_rows)}",
        f"overrides={len(override_rows)}",
        f"unresolved_before_filter={total_unresolved_before_filter}",
        f"unresolved_none={unresolved_none_count}",
        f"missing_url_enrichment={missing_url_enrichment_count}",
        f"skipped_seen={skipped_seen}",
        f"unresolved={len(unresolved_rows)}",
    )

    query_cache = load_query_cache(args.query_cache)
    api_calls_state = {"count": 0}
    cache_hits = 0
    provider_usage: dict[str, int] = {}

    review_rows: list[dict[str, str]] = []
    unmatched_rows: list[dict[str, str]] = []
    accepted_rows: list[dict[str, str]] = []

    review_threshold = max(args.min_confidence - 0.12, 0.55)

    for index, row in enumerate(unresolved_rows, start=1):
        row_fingerprint = unresolved_fingerprint(row)
        seen_unresolved[row_fingerprint] = int(time.time())

        identity = parse_identity(row)
        existing_match_row, _ = match_vivino_row(identity.wine_name, initial_lookup)
        queries = build_queries(identity, row)
        vivino_search_url = build_vivino_search_url(identity)

        candidates_by_url: dict[str, Candidate] = {}
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
                        provider=provider_used,
                        score=score,
                        producer_overlap=producer_overlap,
                        year_match=year_match,
                    )

            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

        ranked = sorted(candidates_by_url.values(), key=lambda c: c.score, reverse=True)
        best = ranked[0] if ranked else None
        second = ranked[1] if len(ranked) > 1 else None

        best_score = best.score if best else 0.0
        second_score = second.score if second else 0.0
        margin = best_score - second_score

        if args.provider == "none":
            decision = "no_provider"
            reason = "provider=none; generated deterministic queries only"
        elif best is None:
            decision = "unmatched"
            reason = "no viable vivino candidates returned"
        elif best.producer_overlap == 0:
            decision = "needs_review"
            reason = "top candidate missing producer token overlap"
        elif best.score >= args.min_confidence and margin >= args.min_margin:
            decision = "auto_accept"
            reason = f"score={best.score:.3f}, margin={margin:.3f}"
        elif best.score >= review_threshold:
            decision = "needs_review"
            reason = f"score={best.score:.3f}, margin={margin:.3f}"
        else:
            decision = "unmatched"
            reason = f"score below threshold ({best.score:.3f} < {review_threshold:.3f})"

        if search_errors:
            reason = f"{reason}; search_error={search_errors[0]}"

        if decision == "auto_accept" and best is not None:
            existing_rating_for_accept = (existing_match_row.get("vivino_rating") or "").strip()
            existing_count_for_accept = (
                (existing_match_row.get("vivino_num_ratings") or "").strip()
                or (existing_match_row.get("vivino_raters") or "").strip()
            )
            if not existing_rating_for_accept and not existing_count_for_accept:
                decision = "needs_review"
                reason = f"{reason}; missing vivino rating/count for auto-apply"

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
            existing_rating = (existing_match_row.get("vivino_rating") or "").strip()
            existing_count = (
                (existing_match_row.get("vivino_num_ratings") or "").strip()
                or (existing_match_row.get("vivino_raters") or "").strip()
            )
            existing_price = (existing_match_row.get("vivino_price") or "").strip()
            existing_name = (existing_match_row.get("wine_name") or "").strip()

            accepted_rows.append(
                {
                    "match_name": identity.wine_name,
                    "wine_name": existing_name or best.title,
                    "vivino_rating": existing_rating,
                    "vivino_num_ratings": existing_count,
                    "vivino_price": existing_price,
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
    write_csv_rows(args.output_suggestions, accepted_rows, _OVERRIDE_FIELDS)

    applied_overrides_rows = len(override_rows)
    if args.auto_apply and accepted_rows:
        merged = upsert_overrides(override_rows, accepted_rows)
        write_csv_rows(args.vivino_overrides, merged, _OVERRIDE_FIELDS)
        applied_overrides_rows = len(merged)

    save_query_cache(args.query_cache, query_cache)
    save_state(args.state_file, state)

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
        f"cache_path={args.query_cache}",
        f"state_path={args.state_file}",
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
        "--query-cache",
        type=Path,
        default=Path("data/vivino_query_cache.json"),
        help="Local JSON cache for provider query results.",
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
        help="Only query unresolved rows never seen before in --state-file (default: true).",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=Path("data/vivino_resolver_state.json"),
        help="Persistent state for delta-only unresolved processing.",
    )
    parser.add_argument("--max-results", type=int, default=8)
    parser.add_argument("--sleep-seconds", type=float, default=1.2)
    parser.add_argument("--min-confidence", type=float, default=0.82)
    parser.add_argument("--min-margin", type=float, default=0.08)
    parser.add_argument("--limit", type=int, default=0, help="Optional max unresolved wines to process (0 means all).")
    parser.add_argument("--auto-apply", action="store_true", help="Append auto-accepted rows into --vivino-overrides")
    parser.add_argument("--output-review", type=Path, default=Path("data/vivino_review_queue.csv"))
    parser.add_argument("--output-unmatched", type=Path, default=Path("data/vivino_unmatched.csv"))
    parser.add_argument("--output-suggestions", type=Path, default=Path("data/vivino_auto_overrides.csv"))
    args = parser.parse_args()

    args.query_cache = args.query_cache.resolve()
    args.state_file = args.state_file.resolve()

    resolve_matches(args)


if __name__ == "__main__":
    main()
