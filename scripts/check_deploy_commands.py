"""Validate checked-in deploy commands against the current CLI parser."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
DEPLOY_DIR = ROOT / "deploy"
REQUIRED_FLAGS = {
    "daily-ingest-command.txt": {
        "--scrape-and-build",
        "--resolve-vivino",
        "--resolver-only-new-unresolved",
        "--llm-resolve-grandcru",
        "--no-validate-retailer-price-math",
    },
    "weekly-ingest-command.txt": {
        "--scrape-and-build",
        "--resolve-vivino",
        "--no-resolver-only-new-unresolved",
        "--llm-resolve-grandcru",
        "--no-validate-retailer-price-math",
    }
}
SERVICE_BY_FILE = {
    "daily-ingest-command.txt": "daily-ingest",
    "weekly-ingest-command.txt": "weekly-ingest",
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


def command_tokens(text: str) -> list[str]:
    return shlex.split(text)


def railway_token() -> str:
    token = os.getenv("RAILWAY_TOKEN", "").strip()
    if token:
        return token

    config_path = Path.home() / ".railway" / "config.json"
    if not config_path.exists():
        raise RuntimeError("RAILWAY_TOKEN is not set and Railway CLI config was not found")
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    token = str(payload.get("user", {}).get("token") or "").strip()
    if not token:
        raise RuntimeError("Railway CLI config does not contain a user token")
    return token


def service_instance_ids(service: str, environment: str) -> tuple[str, str]:
    railway = shutil.which("railway")
    if not railway:
        raise RuntimeError("railway CLI is not installed")

    result = subprocess.run(
        [
            railway,
            "variables",
            "--service",
            service,
            "--environment",
            environment,
            "--json",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)

    payload = json.loads(result.stdout)
    service_id = str(payload.get("RAILWAY_SERVICE_ID") or "").strip()
    environment_id = str(payload.get("RAILWAY_ENVIRONMENT_ID") or "").strip()
    if not service_id or not environment_id:
        raise RuntimeError(f"Missing Railway IDs for service {service!r}")
    return service_id, environment_id


def live_start_command(service: str, environment: str) -> str:
    service_id, environment_id = service_instance_ids(service, environment)
    query = (
        "query serviceInstance($serviceId: String!, $environmentId: String!) {"
        " serviceInstance(serviceId: $serviceId, environmentId: $environmentId) {"
        " startCommand } }"
    )
    body = json.dumps(
        {
            "query": query,
            "variables": {
                "serviceId": service_id,
                "environmentId": environment_id,
            },
        }
    ).encode("utf-8")
    request = Request(
        "https://backboard.railway.com/graphql/v2",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {railway_token()}",
            "Content-Type": "application/json",
            "User-Agent": "railway-cli",
        },
    )
    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if payload.get("errors"):
        raise RuntimeError(payload["errors"])
    return str(payload.get("data", {}).get("serviceInstance", {}).get("startCommand") or "")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live",
        action="store_true",
        help="Also compare checked-in commands with the latest Railway deployment manifests.",
    )
    parser.add_argument("--environment", default="production")
    args = parser.parse_args()

    known_flags = refresh_pipeline_flags()
    findings: list[str] = []

    for path in deploy_command_files():
        checked_in_command = command_text(path)
        parts = command_tokens(checked_in_command)
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

        if args.live and path.name in SERVICE_BY_FILE:
            service = SERVICE_BY_FILE[path.name]
            try:
                live_command = live_start_command(service, args.environment)
            except Exception as exc:
                findings.append(f"{service} live command check failed: {exc}")
            else:
                if command_tokens(live_command) != parts:
                    findings.append(
                        f"{service} live startCommand differs from {path.relative_to(ROOT)}"
                    )

    if findings:
        print("Deploy command check failed.", file=sys.stderr)
        for finding in findings:
            print(f"- {finding}", file=sys.stderr)
        return 1

    print("Deploy command check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
