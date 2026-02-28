"""Scrape Grand Cru base site and Platinum portal into CSV files.

This refactor replaces notebook-only scraping with a reusable script:
- Source A: grandcruwines.com (catalog reference)
- Source B: platinum portal domain (configurable)
"""

import argparse
import csv
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


GRANDCRU_NAME_SELECTORS = [
    "a.boost-pfs-filter-product-item-title",
    "a.full-unstyled-link",
    "a.card__heading-link",
]
GRANDCRU_PRICE_SELECTORS = [
    "span.boost-pfs-filter-product-item-sale-price",
    "span.boost-pfs-filter-product-item-regular-price",
    "span.price-item--sale",
    "span.price-item--regular",
]

PLATINUM_NAME_SELECTORS = [
    "a.title",
    "a[href*='/wines/']",
    "a[href*='/wine/']",
    "a[href*='/products/']",
]
PLATINUM_PRICE_SELECTORS = [
    "strong > span.item-price",
    "span.item-price",
    "span[class*='price']",
    "[itemprop='price']",
]
PLATINUM_VIVINO_LINK_SELECTORS = [
    "a[href*='vivino.com']",
]
PLATINUM_VIVINO_HINT_SELECTORS = [
    "[class*='vivino']",
    "[id*='vivino']",
    "[data-vivino-rating]",
    "[data-vivino]",
]

_VIVINO_URL_RE = re.compile(r"https?://[^\s\"'<>()]*vivino\.com[^\s\"'<>()]*", re.IGNORECASE)
_VIVINO_RATING_RE = [
    re.compile(r"vivino[^0-9]{0,24}([0-5](?:\.\d+)?)", re.IGNORECASE),
    re.compile(r"([0-5](?:\.\d+)?)\s*(?:/5)?\s*(?:vivino|rating)", re.IGNORECASE),
]
_VIVINO_COUNT_RE = [
    re.compile(r"([\d,]+(?:\.\d+)?\s*[kKmM]?)\s*(?:ratings?|reviews?)", re.IGNORECASE),
]


@dataclass
class ScrapeResult:
    rows: list[dict[str, str]]
    pages_scraped: int


def make_driver(*, headless: bool, page_load_timeout: int) -> webdriver.Chrome:
    options = webdriver.ChromeOptions()
    chrome_bin = os.getenv("CHROME_BIN", "").strip()
    if chrome_bin:
        options.binary_location = chrome_bin
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(page_load_timeout)
    return driver


def find_first_element(root, selectors: list[str]):
    for selector in selectors:
        try:
            return root.find_element(By.CSS_SELECTOR, selector)
        except NoSuchElementException:
            continue
    return None


def first_non_empty_text(root, selectors: list[str]) -> str:
    for selector in selectors:
        try:
            element = root.find_element(By.CSS_SELECTOR, selector)
            text = (element.text or "").strip()
            if text:
                return text
        except NoSuchElementException:
            continue
    return "N/A"


def _parse_compact_count(value: str) -> int | None:
    raw = (value or "").strip().replace(",", "")
    if not raw:
        return None
    try:
        if raw[-1] in {"k", "K"}:
            return int(float(raw[:-1]) * 1000)
        if raw[-1] in {"m", "M"}:
            return int(float(raw[:-1]) * 1_000_000)
        return int(float(raw))
    except ValueError:
        return None


def extract_platinum_vivino_fields(card) -> dict[str, str]:
    vivino_url = ""
    for selector in PLATINUM_VIVINO_LINK_SELECTORS:
        elements = card.find_elements(By.CSS_SELECTOR, selector)
        for element in elements:
            href = (element.get_attribute("href") or "").strip()
            if href and "vivino.com" in href.lower():
                vivino_url = href
                break
        if vivino_url:
            break

    html = (card.get_attribute("innerHTML") or "").strip()
    if not vivino_url:
        url_match = _VIVINO_URL_RE.search(html)
        if url_match:
            vivino_url = url_match.group(0).strip()

    hint_text_parts: list[str] = []
    for selector in PLATINUM_VIVINO_HINT_SELECTORS:
        for element in card.find_elements(By.CSS_SELECTOR, selector):
            text = (element.text or "").strip()
            if text:
                hint_text_parts.append(text)
            data_rating = (element.get_attribute("data-vivino-rating") or "").strip()
            if data_rating:
                hint_text_parts.append(f"vivino {data_rating}")
            data_count = (element.get_attribute("data-vivino-num-ratings") or "").strip()
            if data_count:
                hint_text_parts.append(f"{data_count} ratings")

    card_text = " ".join((card.text or "").split())
    context = " ".join(hint_text_parts + [card_text])

    # Only attempt numeric extraction when Vivino appears on-card in some form.
    has_vivino_context = "vivino" in context.lower() or "vivino" in html.lower() or bool(vivino_url)
    if not has_vivino_context:
        return {
            "platinum_vivino_rating": "",
            "platinum_vivino_num_ratings": "",
            "platinum_vivino_url": "",
        }

    rating = ""
    for pattern in _VIVINO_RATING_RE:
        match = pattern.search(context)
        if not match:
            continue
        try:
            value = float(match.group(1))
        except ValueError:
            continue
        if 0.0 <= value <= 5.0:
            rating = f"{value:.2f}".rstrip("0").rstrip(".")
            break

    num_ratings = ""
    for pattern in _VIVINO_COUNT_RE:
        match = pattern.search(context)
        if not match:
            continue
        parsed = _parse_compact_count(match.group(1))
        if parsed is None:
            continue
        num_ratings = str(parsed)
        break

    return {
        "platinum_vivino_rating": rating,
        "platinum_vivino_num_ratings": num_ratings,
        "platinum_vivino_url": vivino_url,
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["name", "price", "url"]
    optional_fields = [
        "in_stock",
        "platinum_vivino_rating",
        "platinum_vivino_num_ratings",
        "platinum_vivino_url",
    ]
    for field in optional_fields:
        if any(field in row for row in rows):
            fieldnames.append(field)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def save_page_html(driver: webdriver.Chrome, debug_dir: Path | None, source: str, page: int) -> None:
    if debug_dir is None:
        return
    debug_dir.mkdir(parents=True, exist_ok=True)
    filename = debug_dir / f"{source}_page_{page}.html"
    filename.write_text(driver.page_source, encoding="utf-8")


def _fetch_json(url: str) -> dict:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=30) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def scrape_grandcru(
    driver: webdriver.Chrome,
    *,
    base_url: str,
    max_pages: int | None,
    sleep_seconds: float,
    debug_dir: Path | None,
) -> ScrapeResult:
    # NOTE: Grand Cru catalog pages changed frequently. Shopify's products.json endpoint
    # is significantly more stable than DOM selectors and captures full catalog pages.
    _ = driver
    _ = debug_dir

    rows: list[dict[str, str]] = []
    seen_names: set[str] = set()
    page = 1
    pages_scraped = 0

    base = base_url.rstrip("/")
    limit = 250

    while True:
        if max_pages is not None and page > max_pages:
            break

        url = f"{base}/products.json?limit={limit}&page={page}"
        print(f"[grandcru] page {page}: {url}")

        try:
            payload = _fetch_json(url)
        except Exception as exc:
            print(f"[grandcru] products.json fetch error on page {page}: {exc}")
            break

        products = payload.get("products") or []
        if not products:
            print(f"[grandcru] no products on page {page}; stopping.")
            break

        page_new_rows = 0
        for product in products:
            name = (product.get("title") or "").strip()
            handle = (product.get("handle") or "").strip()
            if not name or not handle:
                continue

            variants = product.get("variants") or []
            available_variants = [variant for variant in variants if variant.get("available")]
            in_stock = bool(available_variants)
            price_variant = available_variants[0] if available_variants else (variants[0] if variants else {})
            price = str(price_variant.get("price") or "N/A")

            key = name.lower()
            if key in seen_names:
                continue

            seen_names.add(key)
            rows.append(
                {
                    "name": name,
                    "price": price,
                    "url": f"{base}/products/{handle}",
                    "in_stock": "true" if in_stock else "false",
                }
            )
            page_new_rows += 1

        print(f"[grandcru] captured {page_new_rows} new rows on page {page}.")
        pages_scraped += 1

        if len(products) < limit:
            break

        page += 1
        time.sleep(max(sleep_seconds, 0.3))

    return ScrapeResult(rows=rows, pages_scraped=pages_scraped)


def click_in_stock_filter(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href='#']")))
        element = driver.execute_script(
            """
            return [...document.querySelectorAll("a[href='#']")].find(
                a => a.textContent.trim().toLowerCase() === "in stock"
            );
            """
        )
        if element:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
            time.sleep(0.4)
            driver.execute_script("arguments[0].click();", element)
            time.sleep(1.8)
            print("[platinum] clicked 'In stock' filter.")
        else:
            print("[platinum] 'In stock' filter not found; continuing.")
    except Exception as exc:
        print(f"[platinum] failed to click 'In stock' filter: {exc}")


def click_next_page(driver: webdriver.Chrome) -> bool:
    candidates = driver.find_elements(By.CSS_SELECTOR, "li.ais-Pagination-item--nextPage a")
    if not candidates:
        return False

    next_button = candidates[0]
    parent = next_button.find_element(By.XPATH, "./..")
    parent_class = (parent.get_attribute("class") or "").lower()
    if "disabled" in parent_class:
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_button)
        time.sleep(0.3)
        next_button.click()
    except Exception:
        driver.execute_script("arguments[0].click();", next_button)
    return True


def is_platinum_in_stock(card) -> bool:
    oos_text_tokens = ("out of stock", "sold out")
    oos_badges = card.find_elements(By.CSS_SELECTOR, "span.oos, .oos, .badge, [class*='oos']")
    for badge in oos_badges:
        text = (badge.text or "").strip().lower()
        if any(token in text for token in oos_text_tokens):
            return False

    full_text = (card.text or "").strip().lower()
    if any(token in full_text for token in oos_text_tokens):
        return False

    add_to_cart_buttons = card.find_elements(
        By.CSS_SELECTOR,
        "a.btn-add-to-cart, button.btn-add-to-cart, [onclick*='addToCart']",
    )
    return bool(add_to_cart_buttons)


def scrape_platinum(
    driver: webdriver.Chrome,
    *,
    base_url: str,
    wines_path: str,
    max_pages: int | None,
    sleep_seconds: float,
    debug_dir: Path | None,
    include_oos: bool,
) -> ScrapeResult:
    rows: list[dict[str, str]] = []
    seen_names: set[str] = set()
    pages_scraped = 0

    base = base_url.rstrip("/")
    path = wines_path if wines_path.startswith("/") else f"/{wines_path}"
    start_url = f"{base}{path}"
    print(f"[platinum] opening {start_url}")
    driver.get(start_url)
    wait = WebDriverWait(driver, 20)
    time.sleep(sleep_seconds)

    click_in_stock_filter(driver, wait)

    page = 1
    while True:
        if max_pages is not None and page > max_pages:
            break

        print(f"[platinum] page {page}")
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a.title, a[href*='/wines/']")))
        except TimeoutException:
            print(f"[platinum] no card links found on page {page}; stopping.")
            break

        save_page_html(driver, debug_dir, "platinum", page)

        cards = driver.find_elements(By.CSS_SELECTOR, "div.card.col-6")
        if not cards:
            cards = driver.find_elements(By.CSS_SELECTOR, "div.card")
        if not cards:
            cards = driver.find_elements(By.CSS_SELECTOR, "article")
        if not cards:
            print(f"[platinum] no cards found on page {page}; stopping.")
            break

        page_new_rows = 0
        page_oos_skipped = 0
        for card in cards:
            in_stock = is_platinum_in_stock(card)
            if not in_stock and not include_oos:
                page_oos_skipped += 1
                continue

            link_element = find_first_element(card, PLATINUM_NAME_SELECTORS)
            if link_element is None:
                continue

            name = (link_element.text or "").strip()
            href = (link_element.get_attribute("href") or "").strip()
            if not name or not href:
                continue

            href = urljoin(base + "/", href)
            price = first_non_empty_text(card, PLATINUM_PRICE_SELECTORS)
            vivino_fields = extract_platinum_vivino_fields(card)
            key = name.lower()
            if key in seen_names:
                continue

            seen_names.add(key)
            payload = {
                "name": name,
                "price": price,
                "url": href,
                "in_stock": "true" if in_stock else "false",
            }
            if vivino_fields.get("platinum_vivino_rating"):
                payload["platinum_vivino_rating"] = vivino_fields["platinum_vivino_rating"]
            if vivino_fields.get("platinum_vivino_num_ratings"):
                payload["platinum_vivino_num_ratings"] = vivino_fields["platinum_vivino_num_ratings"]
            if vivino_fields.get("platinum_vivino_url"):
                payload["platinum_vivino_url"] = vivino_fields["platinum_vivino_url"]
            rows.append(payload)
            page_new_rows += 1

        if include_oos:
            print(f"[platinum] captured {page_new_rows} new rows on page {page}.")
        else:
            print(
                f"[platinum] captured {page_new_rows} new rows on page {page}; "
                f"skipped {page_oos_skipped} out-of-stock cards."
            )
        pages_scraped += 1

        if not click_next_page(driver):
            break
        page += 1
        time.sleep(max(sleep_seconds, 1.5))

    return ScrapeResult(rows=rows, pages_scraped=pages_scraped)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Grand Cru and Platinum wine catalogs")
    parser.add_argument("--grandcru-base-url", default="https://grandcruwines.com")
    parser.add_argument("--platinum-base-url", default="https://platinum.grandcruwines.com")
    parser.add_argument("--platinum-wines-path", default="/wines")
    parser.add_argument("--output-dir", default="seed")
    parser.add_argument("--grandcru-csv", default="grandcru_wines.csv")
    parser.add_argument("--platinum-csv", default="platinum_wines.csv")
    parser.add_argument("--metadata-json", default="scrape_run.json")
    parser.add_argument(
        "--debug-dir",
        default=None,
        help="If set, saves fetched page HTML for selector debugging",
    )
    parser.add_argument("--max-pages", type=int, default=None, help="Optional page cap for both sources")
    parser.add_argument("--sleep-seconds", type=float, default=2.0)
    parser.add_argument("--page-load-timeout", type=int, default=60)
    parser.add_argument(
        "--include-oos",
        action="store_true",
        help="Include out-of-stock Platinum listings (default excludes them)",
    )
    parser.add_argument("--headed", action="store_true", help="Run with browser UI visible")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    debug_dir = Path(args.debug_dir) if args.debug_dir else None

    driver = make_driver(headless=not args.headed, page_load_timeout=args.page_load_timeout)
    try:
        grandcru = scrape_grandcru(
            driver,
            base_url=args.grandcru_base_url,
            max_pages=args.max_pages,
            sleep_seconds=args.sleep_seconds,
            debug_dir=debug_dir,
        )
        platinum = scrape_platinum(
            driver,
            base_url=args.platinum_base_url,
            wines_path=args.platinum_wines_path,
            max_pages=args.max_pages,
            sleep_seconds=args.sleep_seconds,
            debug_dir=debug_dir,
            include_oos=args.include_oos,
        )
    finally:
        driver.quit()

    grandcru_path = output_dir / args.grandcru_csv
    platinum_path = output_dir / args.platinum_csv
    metadata_path = output_dir / args.metadata_json

    write_csv(grandcru_path, grandcru.rows)
    write_csv(platinum_path, platinum.rows)

    metadata = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "grandcru_base_url": args.grandcru_base_url,
        "platinum_base_url": args.platinum_base_url,
        "platinum_wines_path": args.platinum_wines_path,
        "grandcru_rows": len(grandcru.rows),
        "platinum_rows": len(platinum.rows),
        "grandcru_pages_scraped": grandcru.pages_scraped,
        "platinum_pages_scraped": platinum.pages_scraped,
        "max_pages": args.max_pages,
        "include_oos": args.include_oos,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print(f"[done] wrote {len(grandcru.rows)} rows to {grandcru_path}")
    print(f"[done] wrote {len(platinum.rows)} rows to {platinum_path}")
    print(f"[done] wrote metadata to {metadata_path}")


if __name__ == "__main__":
    main()
