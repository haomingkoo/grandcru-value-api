import argparse
import csv
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]


def count_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def run_command(command: str, env: dict[str, str]) -> None:
    args = shlex.split(command)
    if not args:
        raise ValueError(f"Invalid empty command: {command!r}")
    print(f"[refresh] Running: {' '.join(args)}")
    subprocess.run(args, cwd=ROOT, env=env, check=True)


def run_import(comparison_path: Path, vivino_path: Path, env: dict[str, str]) -> None:
    import_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "import_wine_data.py"),
        "--comparison",
        str(comparison_path),
        "--vivino",
        str(vivino_path),
    ]
    print(f"[refresh] Running import with {comparison_path.name} and {vivino_path.name}")
    subprocess.run(import_cmd, cwd=ROOT, env=env, check=True)


def check_health(health_url: str) -> None:
    print(f"[refresh] Checking health: {health_url}")
    try:
        with urlopen(health_url, timeout=20) as response:
            payload = response.read().decode("utf-8")
    except URLError as exc:
        raise RuntimeError(f"Health check failed: {exc}") from exc

    try:
        body = json.loads(payload)
    except json.JSONDecodeError:
        print(f"[refresh] Health response (raw): {payload[:400]}")
        return

    latest = body.get("latest_ingestion") or {}
    print(
        "[refresh] Health OK:",
        f"total_deals={body.get('total_deals')}",
        f"ingestion_stale={body.get('ingestion_stale')}",
        f"latest_status={latest.get('status')}",
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run wine data refresh steps and import into the configured database."
    )
    parser.add_argument(
        "--comparison",
        type=Path,
        default=ROOT / "seed" / "comparison_summary.csv",
        help="Path to comparison_summary CSV.",
    )
    parser.add_argument(
        "--vivino",
        type=Path,
        default=ROOT / "seed" / "vivino_results.csv",
        help="Path to vivino_results CSV.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Optional DATABASE_URL override (useful for direct prod imports).",
    )
    parser.add_argument(
        "--pre-command",
        action="append",
        default=[],
        help="Optional command to run before import. Repeat for multiple commands.",
    )
    parser.add_argument(
        "--health-url",
        default=None,
        help="Optional API /health URL to verify after import.",
    )
    args = parser.parse_args()

    comparison_path = args.comparison.resolve()
    vivino_path = args.vivino.resolve()

    if not comparison_path.exists():
        raise FileNotFoundError(f"Missing comparison CSV: {comparison_path}")
    if not vivino_path.exists():
        raise FileNotFoundError(f"Missing vivino CSV: {vivino_path}")

    env = os.environ.copy()
    if args.database_url:
        env["DATABASE_URL"] = args.database_url

    print(
        "[refresh] Input rows:",
        f"comparison={count_rows(comparison_path)}",
        f"vivino={count_rows(vivino_path)}",
    )

    for command in args.pre_command:
        run_command(command, env)

    run_import(comparison_path, vivino_path, env)

    if args.health_url:
        check_health(args.health_url)

    print("[refresh] Done.")


if __name__ == "__main__":
    main()
