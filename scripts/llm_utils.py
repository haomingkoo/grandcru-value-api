"""Shared LLM utility functions for Groq calls and caching.

Extracted from llm_vivino_resolver.py so other scripts can reuse
the LLM call helpers and cache logic without importing the full resolver.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from urllib.error import HTTPError
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


# ── Identity Cache ────────────────────────────────────────────────────
# Permanent wine→URL mappings. No TTL — entries persist until flagged.
# Separate from data caches (ratings/prices) which have TTLs.

IDENTITY_CACHE_PATH = ROOT / "data" / "identity_cache.json"


def load_identity_cache(path: Path = IDENTITY_CACHE_PATH) -> dict[str, dict]:
    """Load the permanent identity cache."""
    return load_cache(path)


def save_identity_cache(
    cache: dict[str, dict], path: Path = IDENTITY_CACHE_PATH,
) -> None:
    """Persist the identity cache."""
    save_cache(path, cache)


def get_identity(cache: dict[str, dict], wine_name: str) -> dict | None:
    """Get a validated, unflagged identity entry. Returns None if missing/flagged."""
    key = cache_key(wine_name)
    entry = cache.get(key)
    if not entry:
        return None
    if not entry.get("validated", False):
        return None
    if entry.get("flags"):
        return None
    return entry


def set_identity(
    cache: dict[str, dict],
    wine_name: str,
    *,
    vivino_url: str | None = None,
    wine_searcher_url: str | None = None,
    source: str = "",
    validated: bool = True,
) -> None:
    """Upsert an identity entry. Merges URLs — doesn't overwrite existing with None."""
    key = cache_key(wine_name)
    existing = cache.get(key, {})
    entry = {
        "wine_name": wine_name,
        "vivino_url": vivino_url or existing.get("vivino_url"),
        "wine_searcher_url": wine_searcher_url or existing.get("wine_searcher_url"),
        "source": source or existing.get("source", ""),
        "validated": validated,
        "flags": [],
        "resolved_at": time.time(),
    }
    cache[key] = entry


def flag_identity(cache: dict[str, dict], wine_name: str, flag: str) -> None:
    """Flag an identity entry for re-resolution."""
    key = cache_key(wine_name)
    entry = cache.get(key)
    if entry:
        flags = entry.get("flags", [])
        if flag not in flags:
            flags.append(flag)
        entry["flags"] = flags


def needs_resolution(cache: dict[str, dict], wine_name: str) -> bool:
    """True if wine has no validated identity or is flagged."""
    return get_identity(cache, wine_name) is None


# ── LLM (Groq) ─────────────────────────────────────────────────────────


GROQ_CHAT_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
LLM_REQUEST_ATTEMPTS = 4


def call_llm(prompt: str, api_key: str, model: str = GROQ_MODEL) -> str:
    """Send one prompt to Groq and return the reply text.

    The free tier rate-limits per minute, so 429 and 5xx responses wait
    (honouring Retry-After) and retry; any other error is raised.
    """
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        # gpt-oss reasons before answering; a tight cap returns empty content.
        "max_completion_tokens": 2048,
        "reasoning_effort": "low",
    }).encode("utf-8")
    for attempt in range(1, LLM_REQUEST_ATTEMPTS + 1):
        req = Request(GROQ_CHAT_URL, data=body, method="POST", headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "minmax-wine-resolver/1.0",
        })
        try:
            with urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            break
        except HTTPError as e:
            if (e.code != 429 and e.code < 500) or attempt == LLM_REQUEST_ATTEMPTS:
                raise
            wait = float(e.headers.get("retry-after") or 10 * attempt)
            logger.warning("Groq returned %s (attempt %s/%s); waiting %.0fs", e.code, attempt, LLM_REQUEST_ATTEMPTS, wait)
            time.sleep(wait)
    choices = data.get("choices") or []
    if not choices:
        return ""
    return (choices[0].get("message") or {}).get("content") or ""


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
