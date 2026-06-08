"""Fail if tracked files contain obvious plaintext secrets."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAX_BYTES = 1_000_000

SECRET_PATTERNS = [
    ("google_api_key", re.compile(r"\bAIza[0-9A-Za-z_-]{30,}\b")),
    ("brave_api_key", re.compile(r"\bBSA_[0-9A-Za-z_-]{20,}\b")),
    ("openai_api_key", re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b")),
    ("github_token", re.compile(r"\b(?:ghp|github_pat)_[0-9A-Za-z_]{20,}\b")),
    ("private_key", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |)?PRIVATE KEY-----")),
]


def tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return [ROOT / line for line in result.stdout.splitlines() if line.strip()]


def is_binary(path: Path) -> bool:
    chunk = path.read_bytes()[:2048]
    return b"\0" in chunk


def scan_file(path: Path) -> list[tuple[int, str]]:
    if not path.exists() or path.stat().st_size > MAX_BYTES or is_binary(path):
        return []
    findings: list[tuple[int, str]] = []
    text = path.read_text(encoding="utf-8", errors="ignore")
    for line_no, line in enumerate(text.splitlines(), 1):
        for label, pattern in SECRET_PATTERNS:
            if pattern.search(line):
                findings.append((line_no, label))
    return findings


def main() -> int:
    findings: list[str] = []
    for path in tracked_files():
        relative = path.relative_to(ROOT)
        if relative.name == ".env" or (relative.name.startswith(".env.") and relative.name != ".env.example"):
            findings.append(f"{relative}: tracked env file")
            continue
        for line_no, label in scan_file(path):
            findings.append(f"{relative}:{line_no}: possible {label}")

    if findings:
        print("Plaintext secret check failed. Rotate any exposed key and remove it from git.", file=sys.stderr)
        for finding in findings:
            print(f"- {finding}", file=sys.stderr)
        return 1

    print("Plaintext secret check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
