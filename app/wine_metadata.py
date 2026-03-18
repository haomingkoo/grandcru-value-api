from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


_YEAR_PREFIX_RE = re.compile(r"^(?:19|20)\d{2}\s+|^nv\s+", re.IGNORECASE)
_NON_ALNUM_RE = re.compile(r"[^a-z0-9\s]")
_SPACE_RE = re.compile(r"\s+")
_COLOR_MAP = {
    "red": "Red",
    "white": "White",
    "rose": "Rose",
    "orange": "Orange",
}


@dataclass(frozen=True)
class DerivedWineMetadata:
    producer: str | None = None
    label_name: str | None = None
    country: str | None = None
    region: str | None = None
    wine_type: str | None = None
    style_family: str | None = None
    grapes: str | None = None
    offering_type: str | None = None
    origin_label: str | None = None
    origin_latitude: float | None = None
    origin_longitude: float | None = None
    origin_precision: str | None = None
    origin_source: str | None = None
    origin_confidence: str | None = None
    grape_source: str | None = None
    grape_confidence: str | None = None
    metadata_confidence: str | None = None


@dataclass(frozen=True)
class OriginRule:
    patterns: tuple[str, ...]
    country: str
    region: str
    latitude: float
    longitude: float
    precision: str = "region"


@dataclass(frozen=True)
class GrapeRule:
    patterns: tuple[str, ...]
    grapes: str
    source: str = "listing_keyword"
    confidence: str = "high"


_ORIGIN_RULES: tuple[OriginRule, ...] = (
    OriginRule(("fleur de miraval",), "France", "Champagne", 49.05, 4.03),
    OriginRule(("cotes du jura", "domaine pignier"), "France", "Jura", 46.78, 5.67),
    OriginRule(
        (
            "champagne",
            "cote des bar",
            "la rogerie",
            "jerome blin",
            "charles heidsieck",
            "coutier",
            "william saintot",
            "louise brison",
        ),
        "France",
        "Champagne",
        49.05,
        4.03,
    ),
    OriginRule(("cotes de provence", "miraval"), "France", "Provence", 43.83, 6.24),
    OriginRule(
        (
            "chateauneuf du pape",
            "gigondas",
            "cotes du rhone",
            "famille perrin",
            "beaucastel",
            "perrin et fils",
        ),
        "France",
        "Rhone Valley",
        44.14,
        4.81,
    ),
    OriginRule(
        (
            "corton charlemagne",
            "puligny montrachet",
            "bonnes mares",
            "gevrey chambertin",
            "charmes chambertin",
            "meursault",
            "auxey duresses",
            "chassagne montrachet",
            "chambolle musigny",
            "bourgogne hautes cotes de nuits",
            "domaine xavier monnot",
            "pierre boisson",
            "domaine claude dugat",
            "hudelot baillet",
            "la croix de brully",
        ),
        "France",
        "Burgundy",
        47.05,
        4.84,
    ),
    OriginRule(("brauneberger", "markus molitor"), "Germany", "Mosel", 49.92, 6.95),
    OriginRule(("brunello di montalcino", "ammiraglia", "frescobaldi"), "Italy", "Tuscany", 43.33, 11.33),
    OriginRule(("guidalberto", "le difese", "tenuta san guido"), "Italy", "Bolgheri", 43.23, 10.54),
    OriginRule(
        ("gattinara", "langhe", "barbera d alba", "dolcetto d alba", "moscato d asti", "freisa", "roagna", "vajra", "nervi"),
        "Italy",
        "Piedmont",
        44.70,
        8.04,
    ),
    OriginRule(("sicilia doc", "nero d avola", "baglio le mole"), "Italy", "Sicily", 37.60, 14.02),
    OriginRule(("montepulciano", "collefrisio", "vignaquadra"), "Italy", "Abruzzo", 42.35, 13.40),
    OriginRule(("prosecco", "raboso", "botter"), "Italy", "Veneto", 45.44, 11.00),
    OriginRule(("terlaner", "terlano"), "Italy", "Alto Adige", 46.53, 11.25),
    OriginRule(("sonoma mountain", "paul hobbs"), "United States", "Sonoma County", 38.44, -122.71),
    OriginRule(("rhys", "bearwallow", "mt pajaro"), "United States", "California Central Coast", 36.87, -121.56),
    OriginRule(("jonata", "the paring"), "United States", "Santa Ynez Valley", 34.61, -120.08),
    OriginRule(("daou",), "United States", "Paso Robles", 35.63, -120.69),
    OriginRule(("00 wines", "freya hermann"), "United States", "Willamette Valley", 45.22, -123.10),
    OriginRule(("tahbilk", "nagambie lakes"), "Australia", "Victoria", -36.79, 145.15),
    OriginRule(("marlborough", "black cottage", "two rivers"), "New Zealand", "Marlborough", -41.51, 173.96),
    OriginRule(("finca munoz", "felix solis"), "Spain", "Castilla-La Mancha", 39.28, -2.98),
)

_SPARKLING_MARKERS = (
    "champagne",
    "prosecco",
    "blanc de blancs",
    "blanc de noirs",
    "brut",
    "zero dosage",
    "cote des bar",
    "moscato d asti",
    "fleur de miraval",
)

_SWEET_MARKERS = (
    "dessert",
    "sweet",
    "late harvest",
    "ice wine",
    "vin santo",
    "sauternes",
    "tokaji",
    "beerenauslese",
    "trockenbeerenauslese",
    "moscato d asti",
)

_GRAPE_RULES: tuple[GrapeRule, ...] = (
    GrapeRule(("grenache shiraz mourvedre",), "Grenache, Shiraz, Mourvedre"),
    GrapeRule(("cabernet sauvignon",), "Cabernet Sauvignon"),
    GrapeRule(("sauvignon blanc",), "Sauvignon Blanc"),
    GrapeRule(("pinot noir",), "Pinot Noir"),
    GrapeRule(("gevrey chambertin", "chambolle musigny", "bonnes mares", "charmes chambertin"), "Pinot Noir", source="regional_inference", confidence="medium"),
    GrapeRule(("blanc de noirs",), "Pinot Noir, Pinot Meunier", source="style_inference", confidence="medium"),
    GrapeRule(("blanc de blancs",), "Chardonnay", source="style_inference", confidence="medium"),
    GrapeRule(("chardonnay",), "Chardonnay"),
    GrapeRule(("corton charlemagne", "puligny montrachet", "meursault", "chassagne montrachet", "auxey duresses", "bourgogne hautes cotes de nuits blanc"), "Chardonnay", source="regional_inference", confidence="medium"),
    GrapeRule(("viognier",), "Viognier"),
    GrapeRule(("shiraz",), "Shiraz"),
    GrapeRule(("montepulciano",), "Montepulciano"),
    GrapeRule(("freisa",), "Freisa"),
    GrapeRule(("nero d avola",), "Nero d'Avola"),
    GrapeRule(("barbera",), "Barbera"),
    GrapeRule(("dolcetto",), "Dolcetto"),
    GrapeRule(("moscato d asti",), "Moscato Bianco", source="regional_inference", confidence="medium"),
    GrapeRule(("prosecco",), "Glera", source="regional_inference", confidence="medium"),
    GrapeRule(("raboso",), "Raboso"),
    GrapeRule(("brunello di montalcino",), "Sangiovese", source="regional_inference", confidence="medium"),
    GrapeRule(("gattinara",), "Nebbiolo", source="regional_inference", confidence="medium"),
    GrapeRule(("guidalberto",), "Cabernet Sauvignon, Merlot", source="winery_cuvee_inference", confidence="medium"),
    GrapeRule(("le difese",), "Cabernet Sauvignon, Sangiovese", source="winery_cuvee_inference", confidence="medium"),
    GrapeRule(("terlaner classico",), "Pinot Bianco, Chardonnay, Sauvignon Blanc", source="regional_inference", confidence="medium"),
    GrapeRule(("chateauneuf du pape", "gigondas"), "Grenache Blend", source="regional_inference", confidence="medium"),
    GrapeRule(("cotes du rhone",), "Rhone White Blend", source="regional_inference", confidence="medium"),
    GrapeRule(("cotes de provence", "fleur de miraval", "rose alie"), "Rose Blend", source="style_inference", confidence="medium"),
    GrapeRule(("todos", "fenix", "langhe rosso", "red wine", "coleccion de la familia"), "Red Blend", source="style_inference", confidence="medium"),
)


def _normalize_text(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = text.encode("ascii", "ignore").decode("ascii").lower()
    text = text.replace("&", " and ")
    text = _NON_ALNUM_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip()


def _split_listing_name(
    wine_name: str | None,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    parts = [part.strip() for part in (wine_name or "").split(" - ") if part.strip()]
    if not parts:
        return None, None, None, None, None

    color_index = next((idx for idx, value in enumerate(parts) if _normalize_text(value) in _COLOR_MAP), None)
    body_parts = parts[:color_index] if color_index is not None else parts
    if body_parts:
        body_parts = list(body_parts)
        body_parts[0] = _YEAR_PREFIX_RE.sub("", body_parts[0]).strip()

    if len(body_parts) >= 2:
        producer = " - ".join(part for part in body_parts[:-1] if part).strip() or None
        label = body_parts[-1].strip() or None
    elif body_parts:
        producer = body_parts[0].strip() or None
        label = None
    else:
        producer = None
        label = None

    color = parts[color_index] if color_index is not None else None
    volume_label = parts[color_index + 1] if color_index is not None and color_index + 1 < len(parts) else None
    packaging = parts[color_index + 2] if color_index is not None and color_index + 2 < len(parts) else None
    return producer, label, color, volume_label, packaging


def _detect_origin(text: str) -> OriginRule | None:
    for rule in _ORIGIN_RULES:
        if any(pattern in text for pattern in rule.patterns):
            return rule
    return None


def _detect_wine_type(text: str, color: str | None) -> str | None:
    sparkling = any(marker in text for marker in _SPARKLING_MARKERS)
    normalized_color = _normalize_text(color)

    if sparkling and normalized_color == "rose":
        return "Sparkling Rose"
    if sparkling:
        return "Sparkling"
    if normalized_color:
        return _COLOR_MAP.get(normalized_color)
    return None


def _detect_grapes(text: str, wine_type: str | None) -> tuple[str | None, str | None, str | None]:
    for rule in _GRAPE_RULES:
        if any(pattern in text for pattern in rule.patterns):
            return rule.grapes, rule.source, rule.confidence

    if wine_type == "Rose":
        return "Rose Blend", "style_inference", "medium"
    if wine_type == "Sparkling Rose":
        return "Sparkling Rose Blend", "style_inference", "medium"
    return None, None, None


def _detect_style_family(
    text: str,
    wine_type: str | None,
    origin: OriginRule | None,
) -> str | None:
    if origin is not None and origin.region == "Champagne":
        return "Champagne"
    if "champagne" in text:
        return "Champagne"
    if any(marker in text for marker in _SWEET_MARKERS):
        return "Sweet / Dessert"
    if wine_type in {"Sparkling", "Sparkling Rose"}:
        return "Sparkling"
    if wine_type in {"Red", "White", "Rose", "Orange"}:
        return wine_type
    return None


def _combine_metadata_confidence(origin_confidence: str | None, grape_confidence: str | None) -> str | None:
    weights = {
        None: 0,
        "unknown": 0,
        "low": 1,
        "medium": 2,
        "high": 3,
    }
    total = weights.get(origin_confidence, 0) + weights.get(grape_confidence, 0)
    if total >= 5:
        return "high"
    if total >= 3:
        return "medium"
    if total > 0:
        return "low"
    return None


def _parse_volume_liters(volume: str | None) -> float | None:
    text = _normalize_text(volume)
    if not text:
        return None
    if text.endswith("ml"):
        raw = text[:-2].strip()
        try:
            return float(raw) / 1000.0
        except ValueError:
            return None
    if text.endswith("l"):
        raw = text[:-1].strip()
        try:
            return float(raw)
        except ValueError:
            return None
    return None


def _detect_offering_type(
    quantity: int | None,
    volume: str | None,
    packaging: str | None,
    wine_name: str | None,
) -> str:
    normalized = _normalize_text(" ".join(part for part in [packaging, volume, wine_name] if part))
    liters = _parse_volume_liters(volume)

    if quantity is not None and quantity >= 6:
        return "Case"
    if quantity is not None and quantity > 1:
        return "Bundle"
    if "jeroboam" in normalized or (liters is not None and liters > 1.5):
        return "Large Format"
    if "magnum" in normalized or liters == 1.5:
        return "Magnum"
    if "half bottle" in normalized or liters == 0.375:
        return "Half Bottle"
    return "Single Bottle"


def derive_wine_metadata(
    *,
    wine_name: str | None,
    quantity: int | None,
    volume: str | None,
) -> DerivedWineMetadata:
    producer, label, color, _volume_label, packaging = _split_listing_name(wine_name)
    combined_text = _normalize_text(" ".join(part for part in [producer, label, color, wine_name] if part))
    origin = _detect_origin(combined_text)
    wine_type = _detect_wine_type(combined_text, color)
    style_family = _detect_style_family(combined_text, wine_type, origin)
    grapes, grape_source, grape_confidence = _detect_grapes(combined_text, wine_type)
    offering_type = _detect_offering_type(quantity, volume, packaging, wine_name)

    country = origin.country if origin is not None else None
    region = origin.region if origin is not None else None
    origin_label = f"{region}, {country}" if region and country else country
    origin_source = "listing_keyword_heuristic" if origin is not None else None
    origin_confidence = "medium" if origin is not None else None
    metadata_confidence = _combine_metadata_confidence(origin_confidence, grape_confidence)

    return DerivedWineMetadata(
        producer=producer,
        label_name=label,
        country=country,
        region=region,
        wine_type=wine_type,
        style_family=style_family,
        grapes=grapes,
        offering_type=offering_type,
        origin_label=origin_label,
        origin_latitude=origin.latitude if origin is not None else None,
        origin_longitude=origin.longitude if origin is not None else None,
        origin_precision=origin.precision if origin is not None else None,
        origin_source=origin_source,
        origin_confidence=origin_confidence,
        grape_source=grape_source,
        grape_confidence=grape_confidence,
        metadata_confidence=metadata_confidence,
    )
