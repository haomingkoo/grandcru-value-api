from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.config import settings

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
STATE_PATH = DATA_DIR / "ops_refresh_state.json"


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _csv_row_count(path: Path) -> int | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return sum(1 for _ in csv.DictReader(handle))
    except Exception:
        return None


def _tail_lines(path: Path, lines: int) -> str:
    if not path.exists():
        return ""
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            content = handle.readlines()
        return "".join(content[-lines:])
    except Exception:
        return ""


def build_refresh_command(
    *,
    mode: str,
    health_url: str | None,
    strict_health: bool,
) -> list[str]:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "refresh_pipeline.py"),
        "--comparison",
        "seed/comparison_summary.csv",
        "--vivino",
        "seed/vivino_results.csv",
        "--vivino-overrides",
        "seed/vivino_overrides.csv",
    ]

    if mode in {"daily", "weekly"}:
        cmd.extend(
            [
                "--scrape-and-build",
                "--grandcru-base-url",
                "https://grandcruwines.com",
                "--platinum-base-url",
                "https://platwineclub.wineportal.com",
                "--scrape-output-dir",
                "seed/latest_refresh",
                "--scrape-max-pages",
                "500",
                "--scrape-sleep-seconds",
                "0.8",
                "--resolve-vivino",
                "--resolver-provider",
                "brave",
                "--resolver-auto-apply",
            ]
        )

    if mode == "daily":
        cmd.extend(
            [
                "--resolver-max-api-queries",
                "40",
                "--resolver-only-new-unresolved",
            ]
        )
    elif mode == "weekly":
        cmd.extend(
            [
                "--resolver-max-api-queries",
                "300",
                "--no-resolver-only-new-unresolved",
                "--resolver-cache-ttl-hours",
                "0",
            ]
        )
    elif mode == "import_only":
        pass
    else:
        raise ValueError(f"Unsupported mode: {mode}")

    effective_health_url = (health_url or "").strip() or settings.ops_default_health_url.strip()
    if effective_health_url:
        cmd.extend(["--health-url", effective_health_url])
    cmd.append("--health-strict" if strict_health else "--no-health-strict")
    return cmd


class RefreshRunner:
    def __init__(self, state_path: Path = STATE_PATH):
        self._state_path = state_path
        self._lock = threading.Lock()
        self._process: subprocess.Popen[str] | None = None
        self._state = self._load_state()

    def _load_state(self) -> dict[str, Any]:
        if not self._state_path.exists():
            return {"status": "idle"}
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
        return {"status": "idle"}

    def _save_state(self) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state_path.write_text(
            json.dumps(self._state, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            status = dict(self._state)
            if self._process is not None:
                status["pid"] = self._process.pid
            return status

    def is_running(self) -> bool:
        with self._lock:
            if self._process is None:
                return bool(self._state.get("status") == "running")
            return self._process.poll() is None

    def start(self, *, mode: str, health_url: str | None, strict_health: bool, triggered_by: str) -> dict[str, Any]:
        with self._lock:
            if self._process is not None and self._process.poll() is None:
                return dict(self._state)

            run_id = str(uuid.uuid4())
            command = build_refresh_command(mode=mode, health_url=health_url, strict_health=strict_health)
            log_path = DATA_DIR / f"ops_refresh_{run_id}.log"
            state = {
                "run_id": run_id,
                "status": "starting",
                "mode": mode,
                "triggered_by": triggered_by,
                "started_at": _utc_now_iso(),
                "finished_at": None,
                "exit_code": None,
                "command": command,
                "log_path": str(log_path.relative_to(ROOT)),
            }
            self._state = state
            self._save_state()

        def _runner() -> None:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            env = os.environ.copy()
            with log_path.open("w", encoding="utf-8") as log_file:
                process = subprocess.Popen(
                    command,
                    cwd=ROOT,
                    env=env,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                with self._lock:
                    self._process = process
                    self._state["status"] = "running"
                    self._state["pid"] = process.pid
                    self._save_state()

                exit_code = process.wait()

            with self._lock:
                self._state["status"] = "success" if exit_code == 0 else "failed"
                self._state["exit_code"] = exit_code
                self._state["finished_at"] = _utc_now_iso()
                self._state.pop("pid", None)
                self._process = None
                self._save_state()

        threading.Thread(target=_runner, daemon=True, name=f"refresh-run-{mode}").start()
        return self.get_status()

    def tail_log(self, lines: int = 200) -> dict[str, Any]:
        status = self.get_status()
        log_rel = status.get("log_path")
        if not isinstance(log_rel, str) or not log_rel.strip():
            return {"run_id": status.get("run_id"), "log_tail": ""}
        log_path = ROOT / log_rel
        return {"run_id": status.get("run_id"), "log_tail": _tail_lines(log_path, lines)}


def diagnostics_payload(*, refresh_runner: RefreshRunner, total_deals: int, total_snapshots: int) -> dict[str, Any]:
    files = [
        ROOT / "seed" / "comparison_summary.csv",
        ROOT / "seed" / "vivino_results.csv",
        ROOT / "seed" / "vivino_overrides.csv",
        ROOT / "data" / "vivino_review_queue.csv",
        ROOT / "data" / "vivino_unmatched.csv",
        ROOT / "data" / "vivino_auto_overrides.csv",
    ]
    file_metrics = []
    for path in files:
        file_metrics.append(
            {
                "path": str(path.relative_to(ROOT)),
                "exists": path.exists(),
                "rows": _csv_row_count(path),
                "size_bytes": path.stat().st_size if path.exists() else None,
            }
        )

    db_url = settings.database_url
    db_scheme = db_url.split("://", 1)[0] if "://" in db_url else "unknown"

    return {
        "timestamp": _utc_now_iso(),
        "app_name": settings.app_name,
        "hostname": os.getenv("HOSTNAME", ""),
        "python_version": sys.version,
        "git_commit": os.getenv("RAILWAY_GIT_COMMIT_SHA", ""),
        "railway_service": os.getenv("RAILWAY_SERVICE_NAME", ""),
        "railway_env": os.getenv("RAILWAY_ENVIRONMENT_NAME", ""),
        "database_scheme": db_scheme,
        "brave_api_key_set": bool(os.getenv("BRAVE_API_KEY", "")),
        "ops_api_key_set": bool(settings.ops_api_key),
        "total_deals": total_deals,
        "total_snapshots": total_snapshots,
        "refresh_status": refresh_runner.get_status(),
        "files": file_metrics,
    }

