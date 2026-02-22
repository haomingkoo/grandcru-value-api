import math
import re


_FLOAT_RE = re.compile(r"-?\d+(?:\.\d+)?")
_INT_RE = re.compile(r"\d[\d,]*")


def parse_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"n/a", "none", "nan"}:
        return None

    match = _FLOAT_RE.search(text.replace(",", ""))
    if not match:
        return None
    return float(match.group(0))


def parse_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)

    text = str(value).strip()
    if not text or text.lower() in {"n/a", "none", "nan"}:
        return None

    match = _INT_RE.search(text)
    if not match:
        return None
    return int(match.group(0).replace(",", ""))


def compute_deal_score(
    price_diff_pct: float | None,
    vivino_rating: float | None,
    vivino_num_ratings: int | None,
) -> float:
    """Score a wine deal from 0-100.

    Inputs:
    - price_diff_pct: Platinum minus Grand Cru in percent (negative means cheaper on Platinum).
    - vivino_rating: Rating out of 5.
    - vivino_num_ratings: Number of ratings used as confidence.
    """
    discount_pct = max(-(price_diff_pct or 0.0), 0.0)
    discount_component = min(discount_pct, 60.0)

    rating = max(min(vivino_rating or 0.0, 5.0), 0.0)
    rating_component = (rating / 5.0) * 30.0

    rating_count = max(vivino_num_ratings or 0, 0)
    confidence_component = min(math.log10(rating_count + 1) / 3.0, 1.0) * 10.0

    return round(discount_component + rating_component + confidence_component, 2)

