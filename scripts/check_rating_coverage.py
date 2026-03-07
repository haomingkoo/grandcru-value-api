import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.import_wine_data import (
    build_vivino_lookup,
    build_vivino_url_index,
    match_vivino_row,
    normalize_vivino_url,
)

def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check rating coverage and ensure N/A only when data is truly missing."
    )
    parser.add_argument("--comparison", type=Path, default=Path("seed/comparison_summary.csv"))
    parser.add_argument("--vivino", type=Path, default=Path("seed/vivino_results.csv"))
    parser.add_argument("--vivino-overrides", type=Path, default=Path("seed/vivino_overrides.csv"))
    parser.add_argument("--min-coverage", type=float, default=0.9)
    parser.add_argument("--max-unrated", type=int, default=0)
    args = parser.parse_args()

    comparison = read_rows(args.comparison)
    vivino_rows = read_rows(args.vivino) + read_rows(args.vivino_overrides)
    lookup = build_vivino_lookup(vivino_rows)
    by_url = build_vivino_url_index(vivino_rows)

    total = 0
    rated = 0
    unrated = []
    bad_unrated = []

    for row in comparison:
        wine_name = (row.get("name_plat") or "").strip()
        if not wine_name:
            continue
        total += 1
        vivino, _ = match_vivino_row(wine_name, lookup)
        vivino_rating = (vivino.get("vivino_rating") or "").strip()
        vivino_num = (vivino.get("vivino_num_ratings") or vivino.get("vivino_raters") or "").strip()
        vivino_url = normalize_vivino_url(vivino.get("vivino_url"))
        if vivino_url and not vivino_rating and not vivino_num:
            url_row = by_url.get(vivino_url)
            if url_row:
                vivino_rating = (url_row.get("vivino_rating") or "").strip()
                vivino_num = (url_row.get("vivino_num_ratings") or url_row.get("vivino_raters") or "").strip()
        platinum_rating = (row.get("platinum_vivino_rating") or "").strip()
        platinum_num = (row.get("platinum_vivino_num_ratings") or "").strip()

        if not vivino_rating and platinum_rating:
            vivino_rating = platinum_rating
        if not vivino_num and platinum_num:
            vivino_num = platinum_num

        has_rating = bool(vivino_rating or vivino_num)
        if has_rating:
            rated += 1
        else:
            unrated.append(wine_name)
            # If Platinum has rating fields but we still consider unrated, that's a bug.
            if platinum_rating or platinum_num:
                bad_unrated.append(wine_name)

    coverage = (rated / total) if total else 0.0
    print(f"total={total} rated={rated} coverage={coverage:.3f}")
    print(f"unrated={len(unrated)} max_unrated={args.max_unrated}")

    if bad_unrated:
        print("BAD_UNRATED (should not happen):")
        for name in bad_unrated:
            print(f"- {name}")
        sys.exit(2)

    if coverage < args.min_coverage:
        print(f"FAIL: coverage {coverage:.3f} < {args.min_coverage:.3f}")
        if unrated:
            print("Unrated examples:")
            for name in unrated[:10]:
                print(f"- {name}")
        sys.exit(1)

    if len(unrated) > args.max_unrated:
        print(f"FAIL: unrated {len(unrated)} > max {args.max_unrated}")
        if unrated:
            print("Unrated examples:")
            for name in unrated[:10]:
                print(f"- {name}")
        sys.exit(1)

    print("PASS")


if __name__ == "__main__":
    main()
