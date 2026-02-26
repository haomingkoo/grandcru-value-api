import argparse
import csv
import random
import re
import sys
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, urlparse

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.by import By

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.import_wine_data import (  # noqa: E402
    build_vivino_lookup,
    canonicalize_key,
    extract_year,
    match_vivino_row,
    parse_float,
    parse_int,
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

_BLOCK_PATTERNS = (
    "attention required",
    "verify you are human",
    "access denied",
    "temporarily blocked",
    "cloudflare",
    "captcha",
)


@dataclass
class Candidate:
    url: str
    title: str
    score: float


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def make_driver(*, headless: bool = True, timeout: int = 35) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(timeout)
    return driver


def pause_with_jitter(base_seconds: float, jitter_seconds: float) -> None:
    duration = max(0.0, base_seconds) + max(0.0, random.uniform(0.0, jitter_seconds))
    time.sleep(duration)


def detect_block_page(driver: webdriver.Chrome) -> tuple[bool, str]:
    try:
        title = (driver.title or "").strip().lower()
    except Exception:
        title = ""
    try:
        body_text = (driver.find_element(By.TAG_NAME, "body").text or "").strip().lower()
    except Exception:
        body_text = ""

    haystack = f"{title}\n{body_text}"
    for pattern in _BLOCK_PATTERNS:
        if pattern in haystack:
            return True, pattern
    return False, ""


def clean_query(name: str) -> str:
    text = name
    text = re.sub(r"\(bundle of\s+\d+\)", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(red|white|rose)\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\bstandard bottle\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d+(?:\.\d+)?\s*(?:ml|l)\b", " ", text, flags=re.IGNORECASE)
    text = text.replace(" - ", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def slug_to_title(url: str) -> str:
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if "w" in parts:
        idx = parts.index("w")
        if idx > 0:
            slug = parts[idx - 1]
            return slug.replace("-", " ").strip()
    if parts:
        return parts[-1].replace("-", " ").strip()
    return ""


def token_set_ratio(a: str, b: str) -> float:
    a_tokens = set(a.split())
    b_tokens = set(b.split())
    if not a_tokens or not b_tokens:
        return 0.0
    inter = sorted(a_tokens & b_tokens)
    if not inter:
        return 0.0
    inter_text = " ".join(inter)
    return max(
        SequenceMatcher(None, inter_text, " ".join(sorted(a_tokens))).ratio(),
        SequenceMatcher(None, inter_text, " ".join(sorted(b_tokens))).ratio(),
    )


def score_candidate(target_name: str, candidate_name: str, candidate_url: str) -> float:
    target = canonicalize_key(target_name)
    candidate = canonicalize_key(candidate_name)
    if not target or not candidate:
        return 0.0

    target_tokens = set(target.split())
    candidate_tokens = set(candidate.split())
    overlap = len(target_tokens & candidate_tokens)
    if overlap == 0:
        return 0.0

    token_ratio = overlap / max(len(target_tokens), len(candidate_tokens))
    seq_ratio = SequenceMatcher(None, target, candidate).ratio()
    set_ratio = token_set_ratio(target, candidate)
    score = (token_ratio * 0.45) + (seq_ratio * 0.2) + (set_ratio * 0.35)

    target_year = extract_year(target_name)
    if target_year is not None:
        parsed = urlparse(candidate_url)
        year_values = parse_qs(parsed.query).get("year", [])
        if any(str(target_year) == value for value in year_values):
            score += 0.07

    return min(score, 1.0)


def extract_candidates_from_search(driver: webdriver.Chrome) -> list[dict[str, str]]:
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/w/']")
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()

    for anchor in anchors:
        href = (anchor.get_attribute("href") or "").strip()
        if not href or "/w/" not in href:
            continue
        if "/search/" in href:
            continue

        normalized = href
        if normalized in seen:
            continue

        text = (anchor.text or "").strip()
        if not text:
            text = (driver.execute_script("return arguments[0].textContent || '';", anchor) or "").strip()
        title = text or slug_to_title(normalized)

        seen.add(normalized)
        candidates.append({"url": normalized, "title": title})

    return candidates


def choose_best_candidate(target_name: str, candidates: list[dict[str, str]], min_score: float) -> Candidate | None:
    best: Candidate | None = None
    for candidate in candidates:
        score = score_candidate(target_name, candidate["title"], candidate["url"])
        if best is None or score > best.score:
            best = Candidate(url=candidate["url"], title=candidate["title"], score=score)

    if best is None or best.score < min_score:
        return None
    return best


def fetch_metrics(driver: webdriver.Chrome, url: str, pause: float) -> tuple[str, str, str]:
    try:
        driver.get(url)
        time.sleep(pause)
        body_text = (driver.find_element(By.TAG_NAME, "body").text or "").strip()
    except Exception:
        return "", "", ""

    rating = ""
    num_ratings = ""
    price = ""

    rating_match = re.search(r"\b([0-5]\.[0-9])\s*(?:\n|\s)+(?:based on all vintages|[0-9,]+\s+ratings)\b", body_text)
    if rating_match:
        rating = rating_match.group(1)

    ratings_match = re.search(r"\b([0-9][0-9,]*)\s+ratings\b", body_text)
    if ratings_match:
        num_ratings = ratings_match.group(1).replace(",", "")
    elif "based on all vintages" in body_text.lower():
        num_ratings = "based on all vintages"

    price_match = re.search(r"\$\s*([0-9]+(?:\.[0-9]{1,2})?)", body_text)
    if price_match:
        price = price_match.group(1)

    return rating, num_ratings, price


def unresolved_wines(
    comparison_rows: list[dict[str, str]],
    vivino_rows: list[dict[str, str]],
    override_rows: list[dict[str, str]],
) -> list[str]:
    lookup = build_vivino_lookup(vivino_rows + override_rows)
    missing: list[str] = []
    for row in comparison_rows:
        wine_name = (row.get("name_plat") or "").strip()
        if not wine_name:
            continue
        vivino, _ = match_vivino_row(wine_name, lookup)
        rating = parse_float(vivino.get("vivino_rating")) if vivino else None
        rating_count = parse_int(vivino.get("vivino_num_ratings")) if vivino else None
        vivino_url = (vivino.get("vivino_url") if vivino else None) or ""
        if rating is None and rating_count is None and not vivino_url.strip():
            missing.append(wine_name)
    return missing


def upsert_overrides(existing: list[dict[str, str]], new_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_name: dict[str, dict[str, str]] = {}
    for row in existing:
        key = (row.get("match_name") or "").strip()
        if key:
            by_name[key] = row

    for row in new_rows:
        key = (row.get("match_name") or "").strip()
        if not key:
            continue
        by_name[key] = row

    merged = list(by_name.values())
    merged.sort(key=lambda r: (r.get("match_name") or ""))
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(description="Suggest Vivino overrides for currently unmatched wines")
    parser.add_argument("--comparison", type=Path, default=Path("seed/comparison_summary.csv"))
    parser.add_argument("--vivino", type=Path, default=Path("seed/vivino_results.csv"))
    parser.add_argument("--vivino-overrides", type=Path, default=Path("seed/vivino_overrides.csv"))
    parser.add_argument("--output", type=Path, default=Path("seed/vivino_overrides_suggested.csv"))
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--min-score", type=float, default=0.64)
    parser.add_argument("--sleep-seconds", type=float, default=8.0)
    parser.add_argument("--sleep-jitter-seconds", type=float, default=6.0)
    parser.add_argument("--apply", action="store_true", help="Upsert suggestions into --vivino-overrides")
    parser.add_argument("--fetch-metrics", action="store_true", help="Open chosen wine page and parse rating/ratings/price")
    parser.add_argument(
        "--manual-approve",
        action="store_true",
        help="Prompt before accepting each candidate so a human approves links.",
    )
    parser.add_argument(
        "--stop-on-block",
        action="store_true",
        help="Fail fast when a block/challenge page is detected.",
    )
    parser.add_argument("--headed", action="store_true", help="Run browser with UI")
    args = parser.parse_args()

    comparison_rows = read_csv_rows(args.comparison)
    vivino_rows = read_csv_rows(args.vivino)
    override_rows = read_csv_rows(args.vivino_overrides)

    missing = unresolved_wines(comparison_rows, vivino_rows, override_rows)
    if args.limit > 0:
        missing = missing[: args.limit]

    print(f"[suggest] unresolved wines to scan: {len(missing)}")

    driver = make_driver(headless=not args.headed)
    suggestions: list[dict[str, str]] = []

    try:
        for idx, wine_name in enumerate(missing, 1):
            query = clean_query(wine_name)
            search_url = f"https://www.vivino.com/en/search/wines?q={quote_plus(query)}"
            print(f"[suggest] {idx}/{len(missing)}: {wine_name}")
            print(f"[suggest] search: {search_url}")

            try:
                driver.get(search_url)
                pause_with_jitter(args.sleep_seconds, args.sleep_jitter_seconds)
            except (TimeoutException, WebDriverException) as exc:
                print(f"[suggest] failed to open search page: {exc}")
                continue

            blocked, trigger = detect_block_page(driver)
            if blocked:
                message = f"[suggest] blocked/challenge page detected while searching ({trigger})"
                if args.stop_on_block:
                    raise RuntimeError(message)
                print(message)
                continue

            candidates = extract_candidates_from_search(driver)
            best = choose_best_candidate(wine_name, candidates, args.min_score)
            if best is None:
                print("[suggest] no candidate above confidence threshold")
                continue

            rating = ""
            num_ratings = ""
            price = ""
            if args.fetch_metrics:
                rating, num_ratings, price = fetch_metrics(driver, best.url, args.sleep_seconds)
                blocked, trigger = detect_block_page(driver)
                if blocked:
                    message = f"[suggest] blocked/challenge page detected while fetching metrics ({trigger})"
                    if args.stop_on_block:
                        raise RuntimeError(message)
                    print(message)
                    continue

            if args.manual_approve:
                print(f"[suggest] candidate: {best.title} ({best.score:.3f})")
                print(f"[suggest] url: {best.url}")
                response = input("[suggest] accept this candidate? [y/N]: ").strip().lower()
                if response not in {"y", "yes"}:
                    print("[suggest] skipped by manual review")
                    continue

            suggestions.append(
                {
                    "match_name": wine_name,
                    "wine_name": best.title,
                    "vivino_rating": rating,
                    "vivino_num_ratings": num_ratings,
                    "vivino_price": price,
                    "vivino_url": best.url,
                    "notes": f"auto-suggest score={best.score:.3f} query={query}",
                }
            )
            print(f"[suggest] selected: {best.title} ({best.score:.3f})")

    finally:
        driver.quit()

    write_csv_rows(args.output, suggestions, _OVERRIDE_FIELDS)
    print(f"[suggest] wrote {len(suggestions)} suggestion rows to {args.output}")

    if args.apply and suggestions:
        merged = upsert_overrides(override_rows, suggestions)
        write_csv_rows(args.vivino_overrides, merged, _OVERRIDE_FIELDS)
        print(f"[suggest] applied suggestions into {args.vivino_overrides} (rows={len(merged)})")


if __name__ == "__main__":
    main()
