"""Validate Grand Cru comparison prices against live product JSON."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.scoring import parse_float, parse_int  # noqa: E402
from scripts.build_comparison_summary import extract_quantity  # noqa: E402


@dataclass(frozen=True)
class LivePrice:
    unit_price: float
    variant_price: float
    variant_quantity: int


@dataclass(frozen=True)
class PriceIssue:
    wine_name: str
    stored_price: float | None
    expected_price: float | None
    live_unit_price: float | None
    url: str
    reason: str


def product_json_url(product_url: str) -> str:
    parsed = urlparse(product_url)
    path = parsed.path.rstrip("/")
    if "/products/" not in path:
        raise ValueError("not a Shopify product URL")
    if path.endswith(".json"):
        json_path = path
    else:
        json_path = f"{path}.json"
    return f"{parsed.scheme}://{parsed.netloc}{json_path}"


def fetch_product_payload(product_url: str, timeout_seconds: float) -> dict:
    request = Request(product_json_url(product_url), headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def live_price_from_payload(payload: dict, product_url: str) -> LivePrice | None:
    product = payload.get("product") if isinstance(payload.get("product"), dict) else payload
    if not isinstance(product, dict):
        return None

    product_text = " ".join(
        str(part or "")
        for part in (
            product.get("title"),
            product.get("handle"),
            product_url,
        )
    )
    product_quantity = extract_quantity(product_text) or 1
    candidates: list[LivePrice] = []

    for variant in product.get("variants") or []:
        price = parse_float(variant.get("price"))
        if price is None or price <= 0:
            continue
        variant_text = " ".join(
            str(part or "")
            for part in (
                variant.get("title"),
                variant.get("option1"),
                variant.get("option2"),
                variant.get("sku"),
            )
        )
        variant_quantity = extract_quantity(variant_text) or product_quantity
        unit_price = round(price / max(variant_quantity, 1), 6)
        candidates.append(
            LivePrice(
                unit_price=unit_price,
                variant_price=price,
                variant_quantity=max(variant_quantity, 1),
            )
        )

    if not candidates:
        return None
    return min(candidates, key=lambda item: item.unit_price)


def fetch_live_catalog(
    *,
    base_url: str,
    max_pages: int,
    timeout_seconds: float,
    sleep_seconds: float,
) -> dict[str, LivePrice]:
    base = base_url.rstrip("/")
    live_prices: dict[str, LivePrice] = {}
    for page in range(1, max_pages + 1):
        url = f"{base}/products.json?limit=250&page={page}"
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
        products = payload.get("products") or []
        if not products:
            break

        for product in products:
            handle = (product.get("handle") or "").strip()
            if not handle:
                continue
            product_url = f"{base}/products/{handle}"
            live_price = live_price_from_payload(product, product_url)
            if live_price is not None:
                live_prices[product_url] = live_price

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return live_prices


def validate_rows(
    rows: list[dict[str, str]],
    *,
    fetch_product=None,
    live_prices: dict[str, LivePrice] | None = None,
    timeout_seconds: float,
    tolerance: float,
) -> list[PriceIssue]:
    issues: list[PriceIssue] = []

    for row in rows:
        url = (row.get("url_main") or "").strip()
        if not url:
            continue
        stored_price = parse_float(row.get("price_main"))
        if stored_price is None:
            continue

        wine_name = (row.get("name_plat") or "").strip() or url
        quantity_plat = parse_int(row.get("quantity_plat")) or 1

        live_price = (live_prices or {}).get(url)
        if live_price is None and fetch_product is not None:
            try:
                payload = fetch_product(url, timeout_seconds)
                live_price = live_price_from_payload(payload, url)
            except Exception as exc:
                issues.append(
                    PriceIssue(
                        wine_name=wine_name,
                        stored_price=stored_price,
                        expected_price=None,
                        live_unit_price=None,
                        url=url,
                        reason=f"live fetch failed: {exc}",
                    )
                )
                continue

        if live_price is None:
            issues.append(
                PriceIssue(
                    wine_name=wine_name,
                    stored_price=stored_price,
                    expected_price=None,
                    live_unit_price=None,
                    url=url,
                    reason="not found in live Grand Cru catalog",
                )
            )
            continue

        expected_price = round(live_price.unit_price * max(quantity_plat, 1), 2)
        if abs(stored_price - expected_price) > tolerance:
            issues.append(
                PriceIssue(
                    wine_name=wine_name,
                    stored_price=stored_price,
                    expected_price=expected_price,
                    live_unit_price=round(live_price.unit_price, 2),
                    url=url,
                    reason=(
                        f"expected {expected_price:.2f} from live unit "
                        f"{live_price.unit_price:.2f} x quantity {quantity_plat}, "
                        f"stored {stored_price:.2f}"
                    ),
                )
            )

    return issues


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate scaled Grand Cru prices before import.")
    parser.add_argument("--comparison", type=Path, default=ROOT / "seed" / "comparison_summary.csv")
    parser.add_argument("--grandcru-base-url", default="https://grandcruwines.com")
    parser.add_argument("--live-max-pages", type=int, default=500)
    parser.add_argument("--sleep-seconds", type=float, default=0.05)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--tolerance", type=float, default=0.05)
    parser.add_argument("--strict", action=argparse.BooleanOptionalAction, default=True)
    args = parser.parse_args()

    rows = read_rows(args.comparison)
    checked = sum(1 for row in rows if (row.get("url_main") or "").strip() and row.get("price_main"))
    live_prices = fetch_live_catalog(
        base_url=args.grandcru_base_url,
        max_pages=args.live_max_pages,
        timeout_seconds=args.timeout_seconds,
        sleep_seconds=args.sleep_seconds,
    )
    issues = validate_rows(
        rows,
        live_prices=live_prices,
        timeout_seconds=args.timeout_seconds,
        tolerance=args.tolerance,
    )

    print(
        f"[retailer-price-math] checked={checked} issues={len(issues)} "
        f"comparison={args.comparison}",
        flush=True,
    )
    for issue in issues:
        print(
            "[retailer-price-math] issue "
            f"wine={issue.wine_name!r} stored={issue.stored_price} "
            f"expected={issue.expected_price} live_unit={issue.live_unit_price} "
            f"reason={issue.reason} url={issue.url}",
            flush=True,
        )

    if issues and args.strict:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
