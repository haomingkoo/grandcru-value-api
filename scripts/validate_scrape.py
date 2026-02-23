import argparse
import csv
import re
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


PRICE_RE = re.compile(r"-?\d+(?:\.\d+)?")


def parse_price(value: str) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text or text.lower() in {"n/a", "none", "nan"}:
        return None
    normalized = text.replace("$", "").replace(",", "")
    match = PRICE_RE.search(normalized)
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def summarize(path: Path) -> None:
    rows = load_rows(path)
    total = len(rows)
    names = [((row.get("name") or "").strip()) for row in rows]
    urls = [((row.get("url") or "").strip()) for row in rows]
    prices = [parse_price((row.get("price") or "").strip()) for row in rows]
    stock_values = [((row.get("in_stock") or "").strip().lower()) for row in rows]

    missing_name = sum(1 for x in names if not x)
    missing_url = sum(1 for x in urls if not x)
    missing_price = sum(1 for x in prices if x is None)
    parsed_price = total - missing_price

    name_counts = Counter(n.lower() for n in names if n)
    url_counts = Counter(u for u in urls if u)
    dup_names = sum(1 for _, c in name_counts.items() if c > 1)
    dup_urls = sum(1 for _, c in url_counts.items() if c > 1)

    host_counts = Counter(urlparse(u).netloc for u in urls if u)

    print(f"\n=== {path} ===")
    print(f"rows: {total}")
    print(f"missing name: {missing_name}")
    print(f"missing url: {missing_url}")
    print(f"price parseable: {parsed_price}/{total}")
    print(f"duplicate names: {dup_names}")
    print(f"duplicate urls: {dup_urls}")
    print(f"url hosts: {dict(host_counts)}")
    if any(stock_values):
        in_stock_true = sum(1 for x in stock_values if x in {"1", "true", "yes", "y"})
        in_stock_false = sum(1 for x in stock_values if x in {"0", "false", "no", "n"})
        in_stock_unknown = total - in_stock_true - in_stock_false
        print(f"in_stock true/false/unknown: {in_stock_true}/{in_stock_false}/{in_stock_unknown}")

    bad_rows: list[tuple[int, str, str, str]] = []
    for idx, row in enumerate(rows, start=2):  # header is line 1
        name = (row.get("name") or "").strip()
        url = (row.get("url") or "").strip()
        price = (row.get("price") or "").strip()
        if not name or not url:
            bad_rows.append((idx, name, price, url))
    if bad_rows:
        print("rows with missing required fields (first 10):")
        for line_no, name, price, url in bad_rows[:10]:
            print(f"  line {line_no}: name={name!r} price={price!r} url={url!r}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate scraped CSV quality")
    parser.add_argument("csv_paths", nargs="+", type=Path)
    args = parser.parse_args()
    for csv_path in args.csv_paths:
        summarize(csv_path)


if __name__ == "__main__":
    main()
