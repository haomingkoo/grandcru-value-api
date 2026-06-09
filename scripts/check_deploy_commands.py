"""Validate checked-in deploy commands against the current CLI parser."""

from __future__ import annotations

import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEPLOY_DIR = ROOT / "deploy"
REQUIRED_FLAGS = {
    "daily-ingest-command.txt": {
        "--scrape-and-build",
        "--resolve-vivino",
        "--resolver-only-new-unresolved",
        "--llm-resolve-grandcru",
    },
    "weekly-ingest-command.txt": {
        "--scrape-and-build",
        "--resolve-vivino",
        "--no-resolver-only-new-unresolved",
        "--llm-resolve-grandcru",
    }
}


def refresh_pipeline_flags() -> set[str]:
    commands = [[sys.executable, str(ROOT / "scripts" / "refresh_pipeline.py"), "--help"]]
    uv = shutil.which("uv")
    if uv:
        commands.append([uv, "run", "python", str(ROOT / "scripts" / "refresh_pipeline.py"), "--help"])

    last_error = ""
    for command in commands:
        result = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            capture_output=True,
        )
        if result.returncode == 0:
            break
        last_error = result.stderr or result.stdout
    else:
        raise RuntimeError(f"Unable to read refresh_pipeline.py --help:\n{last_error}")

    return set(re.findall(r"--[a-z0-9][a-z0-9-]*", result.stdout))


def deploy_command_files() -> list[Path]:
    if not DEPLOY_DIR.exists():
        return []
    return sorted(DEPLOY_DIR.glob("*-command.txt"))


def command_text(path: Path) -> str:
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            lines.append(stripped.rstrip("\\").strip())
    return " ".join(lines)


def main() -> int:
    known_flags = refresh_pipeline_flags()
    findings: list[str] = []

    for path in deploy_command_files():
        parts = shlex.split(command_text(path))
        seen_flags = set()
        for token in parts:
            if not token.startswith("--"):
                continue
            flag = token.split("=", 1)[0]
            seen_flags.add(flag)
            if flag not in known_flags:
                findings.append(f"{path.relative_to(ROOT)} uses unknown refresh_pipeline flag: {flag}")
        for flag in sorted(REQUIRED_FLAGS.get(path.name, set()) - seen_flags):
            findings.append(f"{path.relative_to(ROOT)} is missing required flag: {flag}")

    if findings:
        print("Deploy command check failed.", file=sys.stderr)
        for finding in findings:
            print(f"- {finding}", file=sys.stderr)
        return 1

    print("Deploy command check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
