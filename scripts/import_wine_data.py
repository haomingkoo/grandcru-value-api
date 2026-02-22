import argparse
import csv
import sys
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import delete

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.database import Base, SessionLocal, engine  # noqa: E402
from app.models import IngestionRun, WineDeal  # noqa: E402
from app.scoring import compute_deal_score, parse_float, parse_int  # noqa: E402


def normalize_key(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.lower().strip().split())


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def build_vivino_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    for row in rows:
        key = normalize_key(row.get("wine_name"))
        if key and key not in lookup:
            lookup[key] = row
    return lookup


def to_optional_int(value: str | None) -> int | None:
    number = parse_int(value)
    return number


def import_data(comparison_path: Path, vivino_path: Path) -> None:
    if not comparison_path.exists():
        raise FileNotFoundError(f"comparison_summary missing: {comparison_path}")
    if not vivino_path.exists():
        raise FileNotFoundError(f"vivino_results missing: {vivino_path}")

    Base.metadata.create_all(bind=engine)

    comparison_rows = read_csv_rows(comparison_path)
    vivino_rows = read_csv_rows(vivino_path)
    vivino_lookup = build_vivino_lookup(vivino_rows)

    session = SessionLocal()
    run = IngestionRun(status="running", comparison_rows=len(comparison_rows), vivino_rows=len(vivino_rows))
    session.add(run)
    session.commit()
    session.refresh(run)

    try:
        merged_records: list[WineDeal] = []

        for row in comparison_rows:
            wine_name = (row.get("name_plat") or "").strip()
            if not wine_name:
                continue

            vivino = vivino_lookup.get(normalize_key(wine_name), {})
            vivino_rating = parse_float(vivino.get("vivino_rating"))
            vivino_num_ratings = parse_int(vivino.get("vivino_num_ratings")) or parse_int(vivino.get("vivino_raters"))

            price_platinum = parse_float(row.get("price_plat"))
            price_grand_cru = parse_float(row.get("price_main"))
            price_diff = parse_float(row.get("price_diff"))
            price_diff_pct = parse_float(row.get("price_diff_pct"))

            if price_diff is None and price_platinum is not None and price_grand_cru is not None:
                price_diff = round(price_platinum - price_grand_cru, 2)
            if price_diff_pct is None and price_diff is not None and price_grand_cru not in (None, 0):
                price_diff_pct = round((price_diff / price_grand_cru) * 100.0, 2)

            merged_records.append(
                WineDeal(
                    wine_name=wine_name,
                    vintage=to_optional_int(row.get("year_plat")),
                    quantity=to_optional_int(row.get("quantity_plat")),
                    volume=(row.get("volume_plat") or "").strip() or None,
                    price_platinum=price_platinum,
                    price_grand_cru=price_grand_cru,
                    price_diff=price_diff,
                    price_diff_pct=price_diff_pct,
                    cheaper_side=(row.get("cheaper_side") or "").strip() or None,
                    platinum_url=(row.get("url_plat") or "").strip() or None,
                    grand_cru_url=(row.get("url_main") or "").strip() or None,
                    vivino_url=(vivino.get("vivino_url") or "").strip() or None,
                    vivino_rating=vivino_rating,
                    vivino_num_ratings=vivino_num_ratings,
                    deal_score=compute_deal_score(price_diff_pct, vivino_rating, vivino_num_ratings),
                )
            )

        session.execute(delete(WineDeal))
        session.add_all(merged_records)

        run.status = "success"
        run.finished_at = datetime.now(UTC)
        run.merged_rows = len(merged_records)
        run.details = (
            f"Loaded {len(comparison_rows)} comparison rows and {len(vivino_rows)} vivino rows "
            f"into {len(merged_records)} merged deal records."
        )
        session.commit()
        print(run.details)
    except Exception as exc:
        session.rollback()
        run.status = "failed"
        run.finished_at = datetime.now(UTC)
        run.details = f"Import failed: {exc}"
        session.add(run)
        session.commit()
        raise
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Import wine deal data into database")
    parser.add_argument(
        "--comparison",
        type=Path,
        default=Path("comparison_summary.csv"),
        help="Path to comparison_summary.csv",
    )
    parser.add_argument(
        "--vivino",
        type=Path,
        default=Path("vivino_results.csv"),
        help="Path to vivino_results.csv",
    )
    args = parser.parse_args()
    import_data(args.comparison, args.vivino)


if __name__ == "__main__":
    main()
