"""Extract Platinum prices from every committed seed/comparison_summary.csv.

Output: seed/price_history_saved_lists.csv, loaded once into price_observations at startup
(origin "saved_list", dated by commit time). Commits before the bundle-total fix
(c12a4a0, 2026-03-28) stored per-bottle prices for bundles and are skipped. Grand Cru
prices are left out: their matching and bundle scaling changed between these commits.
"""

from __future__ import annotations

import argparse
import csv
import io
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.price_history import _parse_price, volume_ml  # noqa: E402

SEED = "seed/comparison_summary.csv"
FIRST_TRUSTED_COMMIT = "c12a4a0"
FIELDS = ["commit", "committed_at", "wine_name", "source", "price", "currency", "volume_ml", "quantity", "url"]


def git(*args: str) -> str:
    return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True)


def trusted_commits() -> list[tuple[str, str]]:
    lines = git("log", "--reverse", "--format=%H %cI", "--", SEED).splitlines()
    commits = [tuple(line.split(" ", 1)) for line in lines if line]
    start = next(i for i, (sha, _) in enumerate(commits) if sha.startswith(FIRST_TRUSTED_COMMIT))
    return commits[start:]


def rows_for_commit(sha: str, committed_at: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(git("show", f"{sha}:{SEED}")))
    out = []
    for row in reader:
        name = (row.get("name_plat") or "").strip()
        if not name:
            continue
        common = {
            "commit": sha[:7],
            "committed_at": committed_at,
            "wine_name": name,
            "currency": "SGD",
            "volume_ml": volume_ml(row.get("volume_plat")) or "",
            "quantity": row.get("quantity_plat") or "",
        }
        plat = _parse_price(row.get("price_plat"))
        if plat is not None:
            out.append({**common, "source": "platinum", "price": plat, "url": row.get("url_plat") or ""})
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "seed" / "price_history_saved_lists.csv")
    args = parser.parse_args()
    rows = [r for sha, at in trusted_commits() for r in rows_for_commit(sha, at)]
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} prices from {len({r['commit'] for r in rows})} commits to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
