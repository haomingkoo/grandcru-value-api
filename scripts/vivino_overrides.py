from __future__ import annotations


OVERRIDE_FIELDS = [
    "match_name",
    "wine_name",
    "vivino_rating",
    "vivino_num_ratings",
    "vivino_price",
    "vivino_description",
    "vivino_url",
    "locked",
    "notes",
]

_LOCKED_TRUTHY = {"1", "true", "yes", "y", "locked"}


def is_locked_override_row(row: dict[str, str] | None) -> bool:
    if not row:
        return False

    locked = (row.get("locked") or "").strip().lower()
    if locked in _LOCKED_TRUTHY:
        return True

    notes = (row.get("notes") or "").strip().lower()
    return notes.startswith("manual")


def normalize_override_row(row: dict[str, str]) -> dict[str, str]:
    normalized = {field: (row.get(field) or "").strip() for field in OVERRIDE_FIELDS}
    normalized["match_name"] = normalized["match_name"].strip()
    if is_locked_override_row(normalized):
        normalized["locked"] = "1"
    return normalized


def upsert_overrides(existing: list[dict[str, str]], new_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_name: dict[str, dict[str, str]] = {}

    for row in existing:
        key = (row.get("match_name") or "").strip()
        if key:
            normalized = normalize_override_row(row)
            normalized["match_name"] = key
            by_name[key] = normalized

    for row in new_rows:
        key = (row.get("match_name") or "").strip()
        if not key:
            continue

        prior = by_name.get(key)
        if prior is not None and is_locked_override_row(prior):
            continue

        merged = prior.copy() if prior is not None else {field: "" for field in OVERRIDE_FIELDS}
        for field in OVERRIDE_FIELDS:
            incoming = (row.get(field) or "").strip()
            if incoming:
                merged[field] = incoming
        merged["match_name"] = key
        by_name[key] = normalize_override_row(merged)

    merged_rows = list(by_name.values())
    merged_rows.sort(key=lambda row: row.get("match_name") or "")
    return merged_rows
