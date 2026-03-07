import argparse
import csv
import json
import re
import time
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
        if existing and (existing.get("vivino_rating") or existing.get("vivino_num_ratings")):
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
        if rating or count:
            existing = vivino_by_url.get(url)
            if not existing:
                existing = {
                    "wine_name": (row.get("wine_name") or row.get("match_name") or "").strip(),
                    "vivino_rating": "",
                    "vivino_num_ratings": "",
                    "vivino_price": "",
                    "vivino_url": url,
                }
                vivino_rows.append(existing)
                vivino_by_url[url] = existing
            if rating:
                existing["vivino_rating"] = rating
            if count:
                existing["vivino_num_ratings"] = count
            enriched += 1

        if args.sleep_seconds > 0 and index < len(targets):
            time.sleep(args.sleep_seconds)

    fieldnames = [
        "wine_name",
        "vivino_rating",
        "vivino_num_ratings",
        "vivino_price",
        "vivino_url",
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
