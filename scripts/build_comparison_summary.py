"""Build comparison_summary.csv from scraped Grand Cru + Platinum catalog CSVs."""

import argparse
import csv
import re
from pathlib import Path


def parse_price(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def extract_quantity(value: str | None) -> int | None:
    if not value:
        return None
    lower = value.lower()
    patterns = [
        r"bundle[-_\s]?of[-_\s]?(\d+)",
        r"case[-_\s]?of[-_\s]?(\d+)",
        r"\b(\d+)[-_]?(?:bottles?|btls?)\b",
        r"\b(\d+)[-_]?x\b",
        r"\bx[-_\s]?(\d+)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, lower)
        if match:
            return int(match.group(1))
    return None


def parse_quantity_volume_year(url: str | None, name: str | None = None) -> tuple[int, str, int | None]:
    lower = (url or "").lower()
    quantity = extract_quantity(lower) or 1
    if quantity == 1:
        quantity = extract_quantity(name) or 1
    volume = None
    year = None

    if "/products/" in lower:
        tail = lower.split("/products/", 1)[1]
    elif "/wines/" in lower:
        tail = lower.split("/wines/", 1)[1]
    else:
        tail = lower.rsplit("/", 1)[-1]

    vol_match = re.search(r"(\d+)-(\d+)-l", tail)
    if vol_match:
        volume = f"{vol_match.group(1)}.{vol_match.group(2)}l"
    else:
        vol_match = re.search(r"(\d+)[-_]?ml", tail)
        if vol_match:
            volume = f"{vol_match.group(1)}ml"
    if not volume and name:
        name_lower = name.lower()
        name_vol_match = re.search(r"(\d+(?:\.\d+)?)\s*l\b", name_lower)
        if name_vol_match:
            volume = f"{name_vol_match.group(1)}l"
        else:
            name_vol_match = re.search(r"(\d+)\s*ml\b", name_lower)
            if name_vol_match:
                volume = f"{name_vol_match.group(1)}ml"

    if "magnum" in tail and not volume:
        volume = "1.5l"
    if not volume:
        volume = "750ml"

    year_match = re.search(r"\b(19\d{2}|20[0-3]\d)\b", lower)
    if not year_match and name:
        year_match = re.search(r"\b(19\d{2}|20[0-3]\d)\b", name)
    if year_match:
        year = int(year_match.group(1))

    return quantity, volume, year


def package_type(value: str | None) -> str:
    lower = (value or "").lower()
    gift_markers = (
        "gift-box",
        "gift box",
        "wooden-box",
        "wooden box",
        "2-glasses",
        "2 glasses",
        "w-gift-box",
        "w/ gift box",
        "with gift box",
    )
    if any(marker in lower for marker in gift_markers):
        return "gift_set"
    return "standard"


def normalize_name(name: str | None) -> str:
    if not name:
        return ""
    text = name.lower()
    text = re.sub(r"\b(red|white|rose|rosé)\b", "", text)
    text = re.sub(r"\b(\d+(\.\d+)?\s*l|magnum|standard bottle|half bottle|case|bottles|ml)\b", "", text)
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def label_name(name: str | None) -> str:
    parts = [part.strip() for part in (name or "").split(" - ") if part.strip()]
    if not parts:
        return ""

    color_index = None
    for idx, part in enumerate(parts):
        if part.strip().lower() in {"red", "white", "rose", "rosé"}:
            color_index = idx
            break

    body_parts = parts[:color_index] if color_index is not None else parts
    if body_parts:
        body_parts = list(body_parts)
        body_parts[0] = re.sub(r"^(?:19|20)\d{2}\s+|^nv\s+", "", body_parts[0], flags=re.IGNORECASE).strip()

    if len(body_parts) >= 2:
        return normalize_name(body_parts[-1])
    return normalize_name(name)


def match_similarity(left: dict[str, object], right: dict[str, object]) -> float:
    full_score = jaccard_similarity(str(left["name_clean"]), str(right["name_clean"]))
    left_label = str(left.get("label_clean") or "")
    right_label = str(right.get("label_clean") or "")
    if left_label and right_label:
        label_score = jaccard_similarity(left_label, right_label)
        return min(full_score, label_score)
    return full_score


def is_truthy_stock(value: str | None) -> bool:
    text = (value or "").strip().lower()
    if not text:
        return True
    return text in {"true", "1", "yes", "y", "in_stock", "in stock", "available"}


def jaccard_similarity(a: str, b: str) -> float:
    set_a = set(a.split())
    set_b = set(b.split())
    if not set_a and not set_b:
        return 0.0
    union = set_a | set_b
    if not union:
        return 0.0
    return len(set_a & set_b) / len(union)


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def year_matches(a: int | None, b: int | None) -> bool:
    return (a is None and b is None) or (a == b)


def prepare_rows(rows: list[dict[str, str]], *, enforce_in_stock: bool = False) -> list[dict[str, object]]:
    prepared: list[dict[str, object]] = []
    for row in rows:
        if enforce_in_stock and not is_truthy_stock(row.get("in_stock")):
            continue
        url = (row.get("url") or "").strip()
        name = (row.get("name") or "").strip()
        quantity, volume, year = parse_quantity_volume_year(url, name)
        prepared.append(
            {
                "name": name,
                "price": (row.get("price") or "").strip(),
                "url": url,
                "quantity": quantity,
                "volume": volume,
                "year": year,
                "package_type": package_type(" ".join([name, url])),
                "name_clean": normalize_name(name),
                "label_clean": label_name(name),
                "platinum_vivino_rating": (row.get("platinum_vivino_rating") or "").strip(),
                "platinum_vivino_num_ratings": (row.get("platinum_vivino_num_ratings") or "").strip(),
                "platinum_vivino_url": (row.get("platinum_vivino_url") or "").strip(),
            }
        )
    return prepared


def build_matches(
    grandcru: list[dict[str, object]],
    platinum: list[dict[str, object]],
    *,
    threshold: float,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for plat in platinum:
        best_same_pack = None
        best_same_pack_score = 0.0
        best_cross_pack = None
        best_cross_pack_score = 0.0

        for main in grandcru:
            if not year_matches(plat["year"], main["year"]):
                continue
            if plat["volume"] != main["volume"]:
                continue
            if plat["package_type"] != main["package_type"]:
                continue

            score = match_similarity(plat, main)
            if plat["quantity"] == main["quantity"]:
                if score > best_same_pack_score:
                    best_same_pack = main
                    best_same_pack_score = score
            else:
                if score > best_cross_pack_score:
                    best_cross_pack = main
                    best_cross_pack_score = score

        if best_same_pack is not None and best_same_pack_score >= threshold:
            rows.append(
                {
                    "name_plat": plat["name"],
                    "year_plat": plat["year"],
                    "quantity_plat": plat["quantity"],
                    "volume_plat": plat["volume"],
                    "price_plat": plat["price"],
                    "url_plat": plat["url"],
                    "platinum_vivino_rating": plat.get("platinum_vivino_rating") or "",
                    "platinum_vivino_num_ratings": plat.get("platinum_vivino_num_ratings") or "",
                    "platinum_vivino_url": plat.get("platinum_vivino_url") or "",
                    "name_main": best_same_pack["name"],
                    "year_main": best_same_pack["year"],
                    "quantity_main": best_same_pack["quantity"],
                    "volume_main": best_same_pack["volume"],
                    "price_main": best_same_pack["price"],
                    "url_main": best_same_pack["url"],
                    "match_method": "name_same_bundle",
                    "match_score": round(best_same_pack_score, 4),
                }
            )
        elif best_cross_pack is not None and best_cross_pack_score >= threshold:
            rows.append(
                {
                    "name_plat": plat["name"],
                    "year_plat": plat["year"],
                    "quantity_plat": plat["quantity"],
                    "volume_plat": plat["volume"],
                    "price_plat": plat["price"],
                    "url_plat": plat["url"],
                    "platinum_vivino_rating": plat.get("platinum_vivino_rating") or "",
                    "platinum_vivino_num_ratings": plat.get("platinum_vivino_num_ratings") or "",
                    "platinum_vivino_url": plat.get("platinum_vivino_url") or "",
                    "name_main": best_cross_pack["name"],
                    "year_main": best_cross_pack["year"],
                    "quantity_main": best_cross_pack["quantity"],
                    "volume_main": best_cross_pack["volume"],
                    "price_main": best_cross_pack["price"],
                    "url_main": best_cross_pack["url"],
                    "match_method": "name_cross_bundle",
                    "match_score": round(best_cross_pack_score, 4),
                }
            )
        else:
            best_score = max(best_same_pack_score, best_cross_pack_score)
            rows.append(
                {
                    "name_plat": plat["name"],
                    "year_plat": plat["year"],
                    "quantity_plat": plat["quantity"],
                    "volume_plat": plat["volume"],
                    "price_plat": plat["price"],
                    "url_plat": plat["url"],
                    "platinum_vivino_rating": plat.get("platinum_vivino_rating") or "",
                    "platinum_vivino_num_ratings": plat.get("platinum_vivino_num_ratings") or "",
                    "platinum_vivino_url": plat.get("platinum_vivino_url") or "",
                    "name_main": None,
                    "year_main": None,
                    "quantity_main": None,
                    "volume_main": None,
                    "price_main": None,
                    "url_main": "",
                    "match_method": "no_match",
                    "match_score": round(best_score, 4),
                }
            )
    return rows


def build_summary(matched_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    summary_rows: list[dict[str, object]] = []
    for row in matched_rows:
        quantity_plat = int(row.get("quantity_plat") or 1)
        quantity_main = int(row.get("quantity_main") or 1)

        price_plat_total = parse_price(row.get("price_plat"))
        price_main_total = parse_price(row.get("price_main"))

        # Platinum's listed price is the anchor — show as-is (the total
        # the user pays, e.g. $600 for a bundle of 3).
        # Scale Grand Cru to match Platinum's quantity for apples-to-apples:
        # GC per-bottle × Platinum quantity (e.g. $210 × 3 = $630).
        price_plat_num = price_plat_total

        if price_main_total is not None:
            per_bottle_main = price_main_total / max(quantity_main, 1)
            price_main_num = round(per_bottle_main * max(quantity_plat, 1), 2)
        else:
            price_main_num = None

        price_diff = None
        if price_plat_num is not None and price_main_num is not None:
            price_diff = round(price_plat_num - price_main_num, 2)

        price_diff_pct = None
        if price_diff is not None and price_main_num not in (None, 0):
            price_diff_pct = round((price_diff / price_main_num) * 100, 2)

        if price_main_num is None:
            cheaper_side = "No Match"
        elif price_diff is None or price_diff == 0:
            cheaper_side = "Same Price"
        elif price_diff < 0:
            cheaper_side = "Platinum Cheaper"
        else:
            cheaper_side = "Grand Cru Cheaper"

        summary_rows.append(
            {
                "name_plat": row["name_plat"],
                "year_plat": row["year_plat"],
                "quantity_plat": row["quantity_plat"],
                "volume_plat": row["volume_plat"],
                "quantity_main": quantity_main,
                "price_plat": f"{price_plat_num:.2f}" if price_plat_num is not None else row["price_plat"],
                "price_main": f"{price_main_num:.2f}" if price_main_num is not None else row["price_main"],
                "price_diff": price_diff,
                "price_diff_pct": price_diff_pct,
                "cheaper_side": cheaper_side,
                "url_plat": row["url_plat"],
                "url_main": row["url_main"],
                "platinum_vivino_rating": row.get("platinum_vivino_rating") or "",
                "platinum_vivino_num_ratings": row.get("platinum_vivino_num_ratings") or "",
                "platinum_vivino_url": row.get("platinum_vivino_url") or "",
            }
        )

    priority = {
        "Platinum Cheaper": 0,
        "Grand Cru Cheaper": 1,
        "Same Price": 2,
        "No Match": 3,
    }

    def sort_key(row: dict[str, object]) -> tuple[float, float, float]:
        p = priority.get(str(row["cheaper_side"]), 99)
        pct = row["price_diff_pct"]
        pct_key = -pct if isinstance(pct, (int, float)) else float("inf")
        plat = parse_price(row.get("price_plat"))
        plat_key = plat if plat is not None else float("inf")
        return (p, pct_key, plat_key)

    summary_rows.sort(key=sort_key)
    return summary_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Build comparison_summary from raw scraped catalogs")
    parser.add_argument("--grandcru-csv", required=True, type=Path)
    parser.add_argument("--platinum-csv", required=True, type=Path)
    parser.add_argument("--output-comparison", required=True, type=Path)
    parser.add_argument("--output-matched", default=None, type=Path)
    parser.add_argument("--match-threshold", type=float, default=0.6)
    args = parser.parse_args()

    grandcru_rows = prepare_rows(read_rows(args.grandcru_csv), enforce_in_stock=True)
    platinum_rows = prepare_rows(read_rows(args.platinum_csv), enforce_in_stock=True)
    matched = build_matches(grandcru_rows, platinum_rows, threshold=args.match_threshold)
    summary = build_summary(matched)

    if args.output_matched:
        write_rows(
            args.output_matched,
            [
                "name_plat",
                "year_plat",
                "quantity_plat",
                "volume_plat",
                "price_plat",
                "url_plat",
                "platinum_vivino_rating",
                "platinum_vivino_num_ratings",
                "platinum_vivino_url",
                "name_main",
                "year_main",
                "quantity_main",
                "volume_main",
                "price_main",
                "url_main",
                "match_method",
                "match_score",
            ],
            matched,
        )
    write_rows(
        args.output_comparison,
        [
            "name_plat",
            "year_plat",
            "quantity_plat",
            "volume_plat",
            "quantity_main",
            "price_plat",
            "price_main",
            "price_diff",
            "price_diff_pct",
            "cheaper_side",
            "url_plat",
            "url_main",
            "platinum_vivino_rating",
            "platinum_vivino_num_ratings",
            "platinum_vivino_url",
        ],
        summary,
    )

    print(
        f"Built {len(summary)} comparison rows from "
        f"{len(grandcru_rows)} grandcru rows and {len(platinum_rows)} platinum rows."
    )


if __name__ == "__main__":
    main()
