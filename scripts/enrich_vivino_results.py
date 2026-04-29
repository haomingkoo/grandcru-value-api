import argparse
import csv
import json
import re
import time
from difflib import SequenceMatcher
from html import unescape
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, urlunparse
from urllib.request import Request, urlopen

from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException


AGG_RATING_RE = re.compile(
    r'"aggregateRating"\s*:\s*\{\s*"@type"\s*:\s*"AggregateRating"\s*,\s*'
    r'"ratingValue"\s*:\s*"?(?P<rating>[\d.]+)"?\s*,\s*'
    r'"ratingCount"\s*:\s*"?(?P<count>[\d,]+)"?',
    re.IGNORECASE,
)
RATING_VALUE_RE = re.compile(r'"ratingValue"\s*:\s*"?(?P<rating>[\d.]+)"?', re.IGNORECASE)
RATING_COUNT_RE = re.compile(r'"ratingCount"\s*:\s*"?(?P<count>[\d,]+)"?', re.IGNORECASE)
REVIEW_COUNT_RE = re.compile(r'"reviewCount"\s*:\s*"?(?P<count>[\d,]+)"?', re.IGNORECASE)
VIVINO_URL_RE = re.compile(r"https?://(?:www\.)?vivino\.com/[^\s\"'<>]+", re.IGNORECASE)

# ── Vivino page text extraction patterns ──────────────────────────────

_TAG_RE = re.compile(r"<[^>]+>")


def _html_to_text(html: str) -> str:
    """Strip HTML tags and normalize whitespace for text-based extraction."""
    text = _TAG_RE.sub("\n", html)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n ", "\n", text)
    return text


def _parse_jsonld(html: str) -> dict:
    """Extract the first JSON-LD Product block from raw HTML."""
    match = re.search(
        r'<script\s+type="application/ld\+json">\s*(\{.*?\})\s*</script>',
        html,
        re.DOTALL,
    )
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    return {}


def parse_vivino_page_extras(html: str) -> dict[str, str]:
    """Extract price, description, grapes, region, wine style, tasting notes
    and the JSON-LD wine name from a Vivino page.

    Works best on Selenium-rendered HTML where visible text is present.
    Falls back to JSON-LD for description/price when text patterns fail.
    """
    extras: dict[str, str] = {}
    text = _html_to_text(html)
    ld = _parse_jsonld(html)

    # --- JSON-LD wine name (for match validation) ---
    ld_name = (ld.get("name") or "").strip()
    if ld_name:
        extras["vivino_wine_name"] = ld_name

    # --- Price ---
    # Priority 1: visible text "$111 ... Price is per bottle"
    price_match = re.search(
        r"\$(\d+(?:\.\d{2})?)\s*\n\s*Price is per bottle",
        text,
    )
    if price_match:
        extras["price"] = price_match.group(1)
    else:
        # Priority 2: first $ amount before "Add to cart"
        cart_pos = text.find("Add to cart")
        if cart_pos > 0:
            prices = re.findall(r"\$(\d+(?:\.\d{2})?)", text[:cart_pos])
            if prices:
                extras["price"] = prices[-1]

    # Priority 3: JSON-LD offers (existing logic)
    if not extras.get("price"):
        offers = ld.get("offers") or []
        if isinstance(offers, dict):
            offers = [offers]
        sgd_price = None
        any_price = None
        for offer in offers:
            low = offer.get("lowPrice") or offer.get("price")
            if not low:
                continue
            try:
                price_val = float(low)
            except (ValueError, TypeError):
                continue
            if offer.get("priceCurrency") == "SGD":
                sgd_price = price_val
                break
            if any_price is None:
                any_price = price_val
        best_price = sgd_price or any_price
        if best_price and 5 <= best_price <= 50000:
            extras["price"] = str(best_price)

    # Validate price is in sane range
    if extras.get("price"):
        try:
            pval = float(extras["price"])
            if pval < 5 or pval > 50000:
                del extras["price"]
        except ValueError:
            del extras["price"]

    # --- Grapes ---
    grapes_match = re.search(r"Grapes?\s*\n\s*(.+?)(?:\n|$)", text)
    if grapes_match:
        grapes = grapes_match.group(1).strip()
        # Filter out false matches (allergens, nav items, etc.)
        if (
            len(grapes) > 2
            and "Contains" not in grapes
            and "sulfite" not in grapes.lower()
            and len(grapes) < 200
        ):
            extras["grapes"] = grapes

    # --- Region ---
    region_match = re.search(
        r"Region\s*\n\s*([\w\s\u00C0-\u024F]+(?:\s*/\s*[\w\s\u00C0-\u024F]+)*)",
        text,
    )
    if region_match:
        region = region_match.group(1).strip()
        if 3 < len(region) < 120:
            extras["region"] = region

    # --- Wine style ---
    style_match = re.search(r"Wine style\s*\n\s*(.+?)(?:\n|$)", text)
    if style_match:
        style = style_match.group(1).strip()
        if 3 < len(style) < 100:
            extras["wine_style"] = style

    # --- Description ---
    # Priority 1: JSON-LD description
    desc = (ld.get("description") or "").strip()
    if len(desc) > 15:
        extras["description"] = desc[:500]

    # Priority 2: Wine style from HTML (already captured above)
    if not extras.get("description") and extras.get("wine_style"):
        extras["description"] = extras["wine_style"]

    # --- Tasting notes (flavor keywords from rendered text) ---
    taste_section = re.search(
        r"WINE LOVERS TASTE SUMMARY\s*\n.*?\n(.*?)(?:Food that goes well|Wine style|Are you cooking|Trusted by millions)",
        text,
        re.DOTALL,
    )
    if taste_section:
        notes_text = taste_section.group(1)
        flavors = re.findall(
            r"([A-Za-z][^\n\d]{2,60})\n\s*(\d+)\s+mentions?\s+of\s+([\w\s]+?)notes?",
            notes_text,
        )
        if flavors:
            parts = [
                f"{kw.strip()} ({cat.strip()})"
                for kw, _count, cat in flavors[:6]
            ]
            extras["tasting_notes"] = "; ".join(parts)

    # Build rich description by combining sources
    rich_parts: list[str] = []
    if extras.get("description"):
        rich_parts.append(extras["description"])
    if extras.get("tasting_notes"):
        rich_parts.append(f"Tasting: {extras['tasting_notes']}")
    if rich_parts:
        extras["description"] = ". ".join(rich_parts)[:500]

    return extras


def fetch_html(url: str, timeout_seconds: float, user_agent: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        body = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
    return body.decode(charset, errors="ignore")


def make_driver(*, headless: bool = True, timeout: int = 35, user_agent: str | None = None) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    if user_agent:
        options.add_argument(f"--user-agent={user_agent}")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(timeout)
    return driver


def fetch_html_selenium(
    driver: webdriver.Chrome,
    url: str,
    pause_seconds: float,
) -> str:
    driver.get(url)
    if pause_seconds > 0:
        time.sleep(pause_seconds)
    return driver.page_source or ""


def parse_rating_count(html: str) -> tuple[str, str]:
    text = unescape(html)
    match = AGG_RATING_RE.search(text)
    if match:
        return match.group("rating"), match.group("count").replace(",", "")

    rating = ""
    count = ""

    match_rating = RATING_VALUE_RE.search(text)
    if match_rating:
        rating = match_rating.group("rating")

    match_count = RATING_COUNT_RE.search(text) or REVIEW_COUNT_RE.search(text)
    if match_count:
        count = match_count.group("count").replace(",", "")

    return rating, count


def normalize_vivino_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    match = VIVINO_URL_RE.search(url)
    if match:
        url = match.group(0)
    parsed = urlparse(url)
    normalized_path = (parsed.path or "").rstrip("/")
    return urlunparse((parsed.scheme or "https", parsed.netloc, normalized_path, "", "", ""))


def needs_vivino_enrichment(row: dict[str, str] | None) -> bool:
    """Return true when a cached Vivino row is missing import-relevant data."""
    if not row:
        return True

    has_rating_data = bool(
        (row.get("vivino_rating") or "").strip()
        or (row.get("vivino_num_ratings") or "").strip()
    )
    has_price = bool((row.get("vivino_price") or "").strip())
    has_description = bool((row.get("vivino_description") or "").strip())
    has_grapes = bool((row.get("vivino_grapes") or "").strip())
    has_region = bool((row.get("vivino_region") or "").strip())

    return not (has_rating_data and has_price and has_description and has_grapes and has_region)


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


_STRIP_TOKENS_RE = re.compile(
    r"\b(?:red|white|rose|750\s*ml|1\.5\s*l|standard\s*bottle|magnum|bundle\s*of\s*\d+)\b",
    re.IGNORECASE,
)
_YEAR_RE = re.compile(r"^\d{4}\s+|^nv\s+", re.IGNORECASE)
_NONALNUM_RE = re.compile(r"[^a-z0-9\s]")


def _normalize_wine_name(name: str) -> str:
    """Reduce a wine name to core tokens for fuzzy comparison."""
    name = _YEAR_RE.sub("", name)
    name = _STRIP_TOKENS_RE.sub("", name)
    name = _NONALNUM_RE.sub("", name.lower())
    return re.sub(r"\s+", " ", name).strip()


def validate_vivino_match(expected_name: str, vivino_name: str) -> float:
    """Return similarity (0-1) between the expected wine name and the
    Vivino JSON-LD name. Low scores indicate a wrong match."""
    norm_expected = _normalize_wine_name(expected_name)
    norm_vivino = _normalize_wine_name(vivino_name)
    if not norm_expected or not norm_vivino:
        return 0.0
    return SequenceMatcher(None, norm_expected, norm_vivino).ratio()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich vivino_results.csv using vivino_overrides URLs."
    )
    parser.add_argument("--vivino", type=Path, default=Path("seed/vivino_results.csv"))
    parser.add_argument("--vivino-overrides", type=Path, default=Path("seed/vivino_overrides.csv"))
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--sleep-seconds", type=float, default=0.8)
    parser.add_argument(
        "--user-agent",
        default=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max URLs to enrich (0 means all).",
    )
    parser.add_argument(
        "--use-selenium",
        action="store_true",
        help="Use Selenium to render Vivino pages before extracting ratings.",
    )
    parser.add_argument(
        "--selenium-pause-seconds",
        type=float,
        default=2.5,
        help="Pause after page load when using Selenium.",
    )
    parser.add_argument(
        "--selenium-headless",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run Selenium in headless mode (default: true).",
    )
    args = parser.parse_args()

    vivino_rows = read_csv_rows(args.vivino)
    overrides = read_csv_rows(args.vivino_overrides)

    if not overrides:
        print("[enrich] no overrides found; nothing to do.")
        return

    vivino_by_url: dict[str, dict[str, str]] = {}
    for row in vivino_rows:
        url = normalize_vivino_url(row.get("vivino_url") or "")
        if url:
            vivino_by_url[url] = row

    targets: list[dict[str, str]] = []
    for row in overrides:
        url = normalize_vivino_url(row.get("vivino_url") or "")
        if not url:
            continue
        existing = vivino_by_url.get(url)
        if not needs_vivino_enrichment(existing):
            continue
        targets.append(row)

    if args.limit and args.limit > 0:
        targets = targets[: args.limit]

    enriched = 0
    fetch_errors = 0
    driver: webdriver.Chrome | None = None

    if args.use_selenium:
        try:
            driver = make_driver(
                headless=args.selenium_headless,
                timeout=int(max(args.timeout_seconds, 5)),
                user_agent=args.user_agent,
            )
        except WebDriverException as exc:
            raise SystemExit(f"[enrich] selenium driver failed: {exc}") from exc

    for index, row in enumerate(targets, start=1):
        url = normalize_vivino_url(row.get("vivino_url") or "")
        if not url:
            continue
        try:
            if driver is not None:
                html = fetch_html_selenium(driver, url, args.selenium_pause_seconds)
            else:
                html = fetch_html(url, timeout_seconds=args.timeout_seconds, user_agent=args.user_agent)
        except (HTTPError, URLError, TimeoutError, TimeoutException, WebDriverException):
            fetch_errors += 1
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)
            continue

        rating, count = parse_rating_count(html)
        page_extras = parse_vivino_page_extras(html)

        if rating or count or page_extras:
            existing = vivino_by_url.get(url)
            if not existing:
                existing = {
                    "wine_name": (row.get("wine_name") or row.get("match_name") or "").strip(),
                    "vivino_rating": "",
                    "vivino_num_ratings": "",
                    "vivino_price": "",
                    "vivino_url": url,
                    "vivino_description": "",
                    "vivino_grapes": "",
                    "vivino_region": "",
                }
                vivino_rows.append(existing)
                vivino_by_url[url] = existing
            if rating:
                existing["vivino_rating"] = rating
            if count:
                existing["vivino_num_ratings"] = count

            # Enrich with page extras (price, description, grapes, region)
            if page_extras.get("price") and not existing.get("vivino_price"):
                existing["vivino_price"] = page_extras["price"]
            if page_extras.get("description") and not existing.get("vivino_description"):
                existing["vivino_description"] = page_extras["description"]
            if page_extras.get("grapes") and not existing.get("vivino_grapes"):
                existing["vivino_grapes"] = page_extras["grapes"]
            region_val = page_extras.get("region", "")
            if region_val and not existing.get("vivino_region"):
                existing["vivino_region"] = region_val

            # --- Match validation (Phase 4) ---
            vivino_name = page_extras.get("vivino_wine_name", "")
            expected_name = existing.get("wine_name", "")
            if vivino_name and expected_name:
                similarity = validate_vivino_match(expected_name, vivino_name)
                if similarity < 0.5:
                    print(
                        f"[enrich] SUSPECT MATCH: "
                        f"expected='{expected_name}' "
                        f"vivino='{vivino_name}' "
                        f"similarity={similarity:.2f} url={url}"
                    )

            enriched += 1

        if args.sleep_seconds > 0 and index < len(targets):
            time.sleep(args.sleep_seconds)

    fieldnames = [
        "wine_name",
        "vivino_rating",
        "vivino_num_ratings",
        "vivino_price",
        "vivino_url",
        "vivino_description",
        "vivino_grapes",
        "vivino_region",
    ]
    write_csv_rows(args.vivino, vivino_rows, fieldnames)

    print(
        "[enrich] done:",
        f"targets={len(targets)}",
        f"enriched={enriched}",
        f"errors={fetch_errors}",
        f"output={args.vivino}",
    )

    if driver is not None:
        driver.quit()


if __name__ == "__main__":
    main()
