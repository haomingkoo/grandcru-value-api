import argparse
import csv
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen
from typing import Any

from sqlalchemy import create_engine, text

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


def run_vivino_resolver(
    *,
    comparison_path: Path,
    vivino_path: Path,
    vivino_overrides_path: Path,
    provider: str,
    auto_apply: bool,
    max_results: int,
    sleep_seconds: float,
    min_confidence: float,
    min_margin: float,
    limit: int,
    max_api_queries: int,
    auto_provider_order: str,
    query_cache: Path,
    cache_ttl_hours: float,
    only_new_unresolved: bool,
    state_file: Path,
    output_review: Path,
    output_unmatched: Path,
    output_suggestions: Path,
    env: dict[str, str],
) -> None:
    resolver_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "resolve_vivino_matches.py"),
        "--comparison",
        str(comparison_path),
        "--vivino",
        str(vivino_path),
        "--vivino-overrides",
        str(vivino_overrides_path),
        "--provider",
        provider,
        "--max-results",
        str(max_results),
        "--sleep-seconds",
        str(sleep_seconds),
        "--min-confidence",
        str(min_confidence),
        "--min-margin",
        str(min_margin),
        "--limit",
        str(limit),
        "--max-api-queries",
        str(max_api_queries),
        "--auto-provider-order",
        auto_provider_order,
        "--query-cache",
        str(query_cache),
        "--cache-ttl-hours",
        str(cache_ttl_hours),
        "--state-file",
        str(state_file),
        "--output-review",
        str(output_review),
        "--output-unmatched",
        str(output_unmatched),
        "--output-suggestions",
        str(output_suggestions),
    ]
    if only_new_unresolved:
        resolver_cmd.append("--only-new-unresolved")
    else:
        resolver_cmd.append("--no-only-new-unresolved")

    if auto_apply:
        resolver_cmd.append("--auto-apply")

    print(f"[refresh] Running vivino resolver ({provider})")
    subprocess.run(resolver_cmd, cwd=ROOT, env=env, check=True)


def resolver_recent(state_file: Path, min_interval_hours: float) -> bool:
    if min_interval_hours <= 0:
        return False
    if not state_file.exists():
        return False
    try:
        payload = json.loads(state_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    last_run = payload.get("last_run_at")
    if not isinstance(last_run, (int, float)):
        return False
    return (time.time() - float(last_run)) < (min_interval_hours * 3600)


def run_import(comparison_path: Path, vivino_path: Path, vivino_overrides_path: Path, env: dict[str, str]) -> None:
    import_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "import_wine_data.py"),
        "--comparison",
        str(comparison_path),
        "--vivino",
        str(vivino_path),
        "--vivino-overrides",
        str(vivino_overrides_path),
    ]
    print(
        f"[refresh] Running import with {comparison_path.name}, {vivino_path.name},"
        f" overrides={vivino_overrides_path.name}"
    )
    subprocess.run(import_cmd, cwd=ROOT, env=env, check=True)


def run_scrape_and_build(
    *,
    grandcru_base_url: str,
    platinum_base_url: str,
    output_dir: Path,
    max_pages: int,
    sleep_seconds: float,
    headed: bool,
    match_threshold: float,
    platinum_detail_ratings: bool,
    platinum_detail_sleep_seconds: float,
    comparison_path: Path,
    env: dict[str, str],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    scrape_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "scrape_sources.py"),
        "--grandcru-base-url",
        grandcru_base_url,
        "--platinum-base-url",
        platinum_base_url,
        "--output-dir",
        str(output_dir),
        "--max-pages",
        str(max_pages),
        "--sleep-seconds",
        str(sleep_seconds),
    ]
    if platinum_detail_ratings:
        scrape_cmd.append("--platinum-detail-ratings")
        scrape_cmd.extend(["--platinum-detail-sleep-seconds", str(platinum_detail_sleep_seconds)])
    if headed:
        scrape_cmd.append("--headed")
    print(f"[refresh] Running scrape into {output_dir}")
    subprocess.run(scrape_cmd, cwd=ROOT, env=env, check=True)

    build_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "build_comparison_summary.py"),
        "--grandcru-csv",
        str(output_dir / "grandcru_wines.csv"),
        "--platinum-csv",
        str(output_dir / "platinum_wines.csv"),
        "--output-comparison",
        str(comparison_path),
        "--match-threshold",
        str(match_threshold),
    ]
    print(f"[refresh] Building comparison summary into {comparison_path}")
    subprocess.run(build_cmd, cwd=ROOT, env=env, check=True)


def run_build_comparison_only(
    *,
    platinum_csv: Path,
    grandcru_csv: Path,
    match_threshold: float,
    comparison_path: Path,
    env: dict[str, str],
) -> None:
    build_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "build_comparison_summary.py"),
        "--grandcru-csv",
        str(grandcru_csv),
        "--platinum-csv",
        str(platinum_csv),
        "--output-comparison",
        str(comparison_path),
        "--match-threshold",
        str(match_threshold),
    ]
    print(
        "[refresh] Building comparison summary from existing CSVs:",
        f"platinum={platinum_csv}",
        f"grandcru={grandcru_csv}",
        f"output={comparison_path}",
    )
    subprocess.run(build_cmd, cwd=ROOT, env=env, check=True)


def check_health(health_url: str) -> bool:
    print(f"[refresh] Checking health: {health_url}")
    try:
        with urlopen(health_url, timeout=20) as response:
            payload = response.read().decode("utf-8")
    except URLError as exc:
        print(f"[refresh] Health check failed: {exc}")
        return False

    try:
        body = json.loads(payload)
    except json.JSONDecodeError:
        print(f"[refresh] Health response (raw): {payload[:400]}")
        return True

    latest = body.get("latest_ingestion") or {}
    print(
        "[refresh] Health OK:",
        f"total_deals={body.get('total_deals')}",
        f"ingestion_stale={body.get('ingestion_stale')}",
        f"latest_status={latest.get('status')}",
    )
    return True


def compute_rating_coverage(database_url: str) -> tuple[int, int, int, int, float]:
    connect_args: dict[str, Any] = {}
    if database_url.startswith("sqlite"):
        connect_args = {"check_same_thread": False}
    engine = create_engine(database_url, connect_args=connect_args)
    with engine.connect() as conn:
        total = conn.execute(text("select count(*) from wine_deals")).scalar() or 0
        rated = conn.execute(
            text(
                "select count(*) from wine_deals "
                "where vivino_rating is not null or vivino_num_ratings is not null"
            )
        ).scalar() or 0
        unrated_with_url = conn.execute(
            text(
                "select count(*) from wine_deals "
                "where vivino_rating is null and vivino_url is not null"
            )
        ).scalar() or 0
        unrated_without_url = conn.execute(
            text(
                "select count(*) from wine_deals "
                "where vivino_rating is null and vivino_url is null"
            )
        ).scalar() or 0
    coverage = (rated / total) if total else 0.0
    return int(total), int(rated), int(unrated_with_url), int(unrated_without_url), float(coverage)


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
        "--vivino-overrides",
        type=Path,
        default=ROOT / "seed" / "vivino_overrides.csv",
        help="Path to manual vivino overrides CSV.",
    )
    parser.add_argument(
        "--resolve-vivino",
        action="store_true",
        help="Run deterministic vivino query generation and matching resolver before import.",
    )
    parser.add_argument(
        "--resolver-provider",
        choices=["none", "auto", "serper", "google_cse", "brave"],
        default="none",
        help="Search provider for resolver (none generates deterministic review queue only).",
    )
    parser.add_argument(
        "--resolver-auto-apply",
        action="store_true",
        help="Auto-append high-confidence matches into --vivino-overrides.",
    )
    parser.add_argument("--resolver-max-results", type=int, default=8)
    parser.add_argument("--resolver-sleep-seconds", type=float, default=1.2)
    parser.add_argument("--resolver-min-confidence", type=float, default=0.82)
    parser.add_argument("--resolver-min-margin", type=float, default=0.08)
    parser.add_argument("--resolver-limit", type=int, default=0)
    parser.add_argument("--resolver-max-api-queries", type=int, default=0)
    parser.add_argument(
        "--resolver-min-interval-hours",
        type=float,
        default=6.0,
        help="Skip resolver run if the previous run was within this many hours.",
    )
    parser.add_argument(
        "--resolver-auto-provider-order",
        default=os.getenv("VIVINO_AUTO_PROVIDER_ORDER", "google_cse,brave,serper"),
    )
    parser.add_argument(
        "--resolver-query-cache",
        type=Path,
        default=ROOT / "data" / "vivino_query_cache.json",
    )
    parser.add_argument("--resolver-cache-ttl-hours", type=float, default=168.0)
    parser.add_argument(
        "--resolver-only-new-unresolved",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only query newly unresolved rows (default: true).",
    )
    parser.add_argument(
        "--resolver-state-file",
        type=Path,
        default=ROOT / "data" / "vivino_resolver_state.json",
    )
    parser.add_argument(
        "--resolver-output-review",
        type=Path,
        default=ROOT / "data" / "vivino_review_queue.csv",
        help="Resolver review queue output CSV path.",
    )
    parser.add_argument(
        "--resolver-output-unmatched",
        type=Path,
        default=ROOT / "data" / "vivino_unmatched.csv",
        help="Resolver unmatched output CSV path.",
    )
    parser.add_argument(
        "--resolver-output-suggestions",
        type=Path,
        default=ROOT / "data" / "vivino_auto_overrides.csv",
        help="Resolver auto-accepted suggestions output CSV path.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Optional DATABASE_URL override (useful for direct prod imports).",
    )
    parser.add_argument(
        "--enrich-vivino-results",
        action="store_true",
        help="Fetch missing vivino ratings/counts for override URLs and update vivino_results.csv.",
    )
    parser.add_argument("--enrich-vivino-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--enrich-vivino-sleep-seconds", type=float, default=0.8)
    parser.add_argument("--enrich-vivino-limit", type=int, default=0)
    parser.add_argument(
        "--ratings-coverage-min",
        type=float,
        default=0.0,
        help="Minimum required fraction of deals with vivino_rating (0-1).",
    )
    parser.add_argument(
        "--ratings-coverage-strict",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Fail the run if ratings coverage is below --ratings-coverage-min.",
    )
    parser.add_argument(
        "--max-unrated",
        type=int,
        default=-1,
        help=(
            "Maximum allowed unrated rows after import. "
            "Set -1 to disable this check (default)."
        ),
    )
    parser.add_argument(
        "--max-unrated-strict",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Fail the run if unrated rows exceed --max-unrated.",
    )
    parser.add_argument(
        "--enrich-platinum-vivino",
        action="store_true",
        help=(
            "Compatibility flag. Vivino-on-Platinum extraction is already handled "
            "inside scripts/scrape_sources.py during --scrape-and-build."
        ),
    )
    parser.add_argument(
        "--build-comparison",
        action="store_true",
        help=(
            "Build comparison summary from existing --platinum/--grandcru CSVs "
            "(without scraping)."
        ),
    )
    parser.add_argument(
        "--platinum",
        type=Path,
        default=ROOT / "seed" / "latest_refresh" / "platinum_wines.csv",
        help="Platinum CSV path used by --build-comparison.",
    )
    parser.add_argument(
        "--grandcru",
        type=Path,
        default=ROOT / "seed" / "latest_refresh" / "grandcru_wines.csv",
        help="Grand Cru CSV path used by --build-comparison.",
    )
    parser.add_argument(
        "--scrape-and-build",
        action="store_true",
        help="Scrape both websites and rebuild comparison summary before resolver/import.",
    )
    parser.add_argument(
        "--grandcru-base-url",
        default="https://grandcruwines.com",
    )
    parser.add_argument(
        "--platinum-base-url",
        default="https://platwineclub.wineportal.com",
    )
    parser.add_argument(
        "--scrape-output-dir",
        type=Path,
        default=ROOT / "seed" / "latest_refresh",
    )
    parser.add_argument("--scrape-max-pages", type=int, default=50)
    parser.add_argument("--scrape-sleep-seconds", type=float, default=1.0)
    parser.add_argument("--platinum-detail-ratings", action="store_true")
    parser.add_argument("--platinum-detail-sleep-seconds", type=float, default=2.0)
    parser.add_argument("--scrape-headed", action="store_true")
    parser.add_argument("--build-match-threshold", type=float, default=0.6)
    parser.add_argument(
        "--pre-command",
        action="append",
        default=[],
        help="Optional command to run before import. Repeat for multiple commands.",
    )
    parser.add_argument(
        "--llm-resolve",
        action="store_true",
        help="Run LLM-powered Vivino resolver (Gemini + Brave) to get descriptions and prices.",
    )
    parser.add_argument("--llm-resolve-limit", type=int, default=0, help="Max wines for LLM resolver (0 = all)")
    parser.add_argument("--llm-resolve-all", action="store_true", help="Resolve ALL wines, not just unmatched")
    parser.add_argument("--llm-resolve-sleep", type=float, default=2.0)
    parser.add_argument("--llm-resolve-force", action="store_true", help="Bypass LLM resolver 30-day cache")
    parser.add_argument(
        "--health-url",
        default=None,
        help="Optional API /health URL to verify after import.",
    )
    parser.add_argument(
        "--health-strict",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="If true, fail the run when --health-url check fails (default: false).",
    )
    args = parser.parse_args()

    comparison_path = args.comparison.resolve()
    vivino_path = args.vivino.resolve()
    vivino_overrides_path = args.vivino_overrides.resolve()

    build_requested = args.scrape_and_build or args.build_comparison

    if not comparison_path.exists() and not build_requested:
        raise FileNotFoundError(f"Missing comparison CSV: {comparison_path}")
    if not vivino_path.exists():
        raise FileNotFoundError(f"Missing vivino CSV: {vivino_path}")

    env = os.environ.copy()
    if args.database_url:
        env["DATABASE_URL"] = args.database_url

    if args.enrich_platinum_vivino:
        print(
            "[refresh] --enrich-platinum-vivino enabled; note: this repo already captures "
            "Platinum Vivino metadata during --scrape-and-build."
        )

    for command in args.pre_command:
        run_command(command, env)

    if args.scrape_and_build:
        run_scrape_and_build(
            grandcru_base_url=args.grandcru_base_url,
            platinum_base_url=args.platinum_base_url,
            output_dir=args.scrape_output_dir.resolve(),
            max_pages=args.scrape_max_pages,
            sleep_seconds=args.scrape_sleep_seconds,
            headed=args.scrape_headed,
            match_threshold=args.build_match_threshold,
            comparison_path=comparison_path,
            platinum_detail_ratings=args.platinum_detail_ratings,
            platinum_detail_sleep_seconds=args.platinum_detail_sleep_seconds,
            env=env,
        )
    elif args.build_comparison:
        platinum_csv = args.platinum.resolve()
        grandcru_csv = args.grandcru.resolve()
        if not platinum_csv.exists():
            raise FileNotFoundError(f"Missing platinum CSV: {platinum_csv}")
        if not grandcru_csv.exists():
            raise FileNotFoundError(f"Missing grandcru CSV: {grandcru_csv}")
        run_build_comparison_only(
            platinum_csv=platinum_csv,
            grandcru_csv=grandcru_csv,
            match_threshold=args.build_match_threshold,
            comparison_path=comparison_path,
            env=env,
        )

    print(
        "[refresh] Input rows:",
        f"comparison={count_rows(comparison_path)}",
        f"vivino={count_rows(vivino_path)}",
        f"overrides={count_rows(vivino_overrides_path) if vivino_overrides_path.exists() else 0}",
    )

    if args.resolve_vivino:
        state_path = args.resolver_state_file.resolve()
        if resolver_recent(state_path, args.resolver_min_interval_hours):
            print(
                f"[refresh] Skipping resolver; last run within {args.resolver_min_interval_hours}h."
            )
        else:
            run_vivino_resolver(
                comparison_path=comparison_path,
                vivino_path=vivino_path,
                vivino_overrides_path=vivino_overrides_path,
                provider=args.resolver_provider,
                auto_apply=args.resolver_auto_apply,
                max_results=args.resolver_max_results,
                sleep_seconds=args.resolver_sleep_seconds,
                min_confidence=args.resolver_min_confidence,
                min_margin=args.resolver_min_margin,
                limit=args.resolver_limit,
                max_api_queries=args.resolver_max_api_queries,
                auto_provider_order=args.resolver_auto_provider_order,
                query_cache=args.resolver_query_cache.resolve(),
                cache_ttl_hours=args.resolver_cache_ttl_hours,
                only_new_unresolved=args.resolver_only_new_unresolved,
                state_file=state_path,
                output_review=args.resolver_output_review.resolve(),
                output_unmatched=args.resolver_output_unmatched.resolve(),
                output_suggestions=args.resolver_output_suggestions.resolve(),
                env=env,
            )

    if args.llm_resolve:
        gemini_key = os.getenv("GEMINI_API_KEY", "") or os.getenv("GOOGLE_API_KEY", "")
        brave_key = os.getenv("BRAVE_API_KEY", "")
        if not gemini_key:
            print("[refresh] Skipping LLM resolver: no GEMINI_API_KEY set")
        else:
            llm_cmd = [
                sys.executable,
                str(ROOT / "scripts" / "llm_vivino_resolver.py"),
                "--comparison", str(comparison_path),
                "--vivino", str(vivino_path),
                "--vivino-overrides", str(vivino_overrides_path),
                "--auto-apply",
                "--sleep", str(args.llm_resolve_sleep),
            ]
            if brave_key:
                llm_cmd.extend(["--brave-api-key", brave_key])
            if args.llm_resolve_force:
                llm_cmd.append("--force")
            if args.llm_resolve_all:
                llm_cmd.append("--all")
            if args.llm_resolve_limit > 0:
                llm_cmd.extend(["--limit", str(args.llm_resolve_limit)])
            print(f"[refresh] Running LLM Vivino resolver (limit={args.llm_resolve_limit or 'all'})")
            subprocess.run(llm_cmd, cwd=ROOT, env=env, check=True)

    if args.enrich_vivino_results:
        enrich_cmd = [
            sys.executable,
            str(ROOT / "scripts" / "enrich_vivino_results.py"),
            "--vivino",
            str(vivino_path),
            "--vivino-overrides",
            str(vivino_overrides_path),
            "--timeout-seconds",
            str(args.enrich_vivino_timeout_seconds),
            "--sleep-seconds",
            str(args.enrich_vivino_sleep_seconds),
        ]
        if args.enrich_vivino_limit:
            enrich_cmd.extend(["--limit", str(args.enrich_vivino_limit)])
        print("[refresh] Enriching vivino_results.csv from override URLs")
        subprocess.run(enrich_cmd, cwd=ROOT, env=env, check=True)

    run_import(comparison_path, vivino_path, vivino_overrides_path, env)

    if args.health_url:
        health_ok = check_health(args.health_url)
        if not health_ok and args.health_strict:
            raise RuntimeError("Health check failed in strict mode.")

    if (args.ratings_coverage_min and args.ratings_coverage_min > 0) or args.max_unrated >= 0:
        database_url = args.database_url or env.get("DATABASE_URL") or "sqlite:///./data/wines.db"
        try:
            total, rated, unrated_with_url, unrated_without_url, coverage = compute_rating_coverage(database_url)
        except Exception as exc:
            if args.ratings_coverage_strict or args.max_unrated_strict:
                raise RuntimeError(f"Ratings coverage check failed: {exc}") from exc
            print(f"[refresh] Ratings coverage check failed: {exc}")
        else:
            unrated = total - rated
            print(
                "[refresh] Ratings coverage:",
                f"rated={rated}",
                f"total={total}",
                f"unrated={unrated}",
                f"unrated_with_url={unrated_with_url}",
                f"unrated_without_url={unrated_without_url}",
                f"coverage={coverage:.3f}",
            )
            if args.ratings_coverage_min > 0 and coverage < args.ratings_coverage_min:
                message = (
                    f"Ratings coverage {coverage:.3f} below minimum "
                    f"{args.ratings_coverage_min:.3f}"
                )
                if args.ratings_coverage_strict:
                    raise RuntimeError(message)
                print(f"[refresh] WARNING: {message}")
            if args.max_unrated >= 0 and unrated > args.max_unrated:
                message = f"Unrated rows {unrated} exceed max {args.max_unrated}"
                if args.max_unrated_strict:
                    raise RuntimeError(message)
                print(f"[refresh] WARNING: {message}")

    print("[refresh] Done.")


if __name__ == "__main__":
    main()
