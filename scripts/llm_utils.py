"""Shared LLM utility functions for Gemini API calls and caching.

Extracted from llm_vivino_resolver.py so other scripts can reuse
the Gemini call helpers and cache logic without importing the full resolver.
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.import_wine_data import canonicalize_key  # noqa: E402

logger = logging.getLogger("grandcru.llm_utils")


# ── Cache ──────────────────────────────────────────────────────────────


def load_cache(path: Path) -> dict[str, dict]:
    """Load the LLM resolver cache from JSON."""
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_cache(path: Path, cache: dict[str, dict]) -> None:
    """Persist the cache to JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def cache_key(wine_name: str) -> str:
    """Stable cache key from a wine name."""
    return canonicalize_key(wine_name)


def is_cache_fresh(entry: dict, ttl_days: int) -> bool:
    """Check if a cache entry is within its TTL."""
    resolved_at = entry.get("resolved_at")
    if not isinstance(resolved_at, (int, float)):
        return False
    age_days = (time.time() - resolved_at) / 86400
    return age_days < ttl_days


# ── Gemini API ─────────────────────────────────────────────────────────


def call_gemini(prompt: str, api_key: str, model: str = "gemini-2.5-flash") -> str:
    """Call Gemini API via REST with thinking disabled for fast, clean output."""
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent"
    )
    body = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 512,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }).encode("utf-8")
    req = Request(url, data=body, method="POST", headers={
        "Content-Type": "application/json",
        "x-goog-api-key": api_key,
    })
    with urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    candidates = data.get("candidates", [])
    if not candidates:
        return ""
    parts = candidates[0].get("content", {}).get("parts", [])
    return parts[0].get("text", "") if parts else ""


def call_gemini_with_search(
    prompt: str, api_key: str, model: str = "gemini-2.5-flash",
) -> str:
    """Call Gemini API with Google Search grounding enabled."""
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent"
    )
    body = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 4096,
        },
    }).encode("utf-8")
    req = Request(url, data=body, method="POST", headers={
        "Content-Type": "application/json",
        "x-goog-api-key": api_key,
    })
    with urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    candidates = data.get("candidates", [])
    if not candidates:
        return ""
    # Take only the first text part — grounding sometimes duplicates.
    parts = candidates[0].get("content", {}).get("parts", [])
    for p in parts:
        if "text" in p:
            return p["text"]
    return ""


def _parse_grounding_json(raw: str) -> dict:
    """Best-effort parse JSON from Gemini grounding, handling code fences
    and truncated responses."""
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        truncated = cleaned.rstrip().rstrip(",")
        if truncated.startswith("{") and not truncated.endswith("}"):
            truncated += "}"
            try:
                return json.loads(truncated)
            except json.JSONDecodeError:
                pass
    return {}
