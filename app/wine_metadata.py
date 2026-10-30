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
            "adrien renoir",
            "dhondt",
            "domaine mumm",
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
            "saint romain",
            "volnay",
            "domaine xavier monnot",
            "henri et gilles buisson",
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
    OriginRule(("quattro passi", "primitivo", "salento"), "Italy", "Puglia", 40.79, 17.10),
    OriginRule(("brunello di montalcino", "ammiraglia", "frescobaldi"), "Italy", "Tuscany", 43.33, 11.33),
    OriginRule(("guidalberto", "le difese", "tenuta san guido"), "Italy", "Bolgheri", 43.23, 10.54),
    OriginRule(
        ("gattinara", "langhe", "barbera d alba", "dolcetto d alba", "moscato d asti", "freisa", "roagna", "vajra", "nervi", "negretti"),
        "Italy",
        "Piedmont",
        44.70,
        8.04,
    ),
    OriginRule(("sicilia doc", "nero d avola", "baglio le mole"), "Italy", "Sicily", 37.60, 14.02),
    OriginRule(("montepulciano", "collefrisio", "vignaquadra"), "Italy", "Abruzzo", 42.35, 13.40),
    OriginRule(("terre d abruzzo", "tombacco pecorino"), "Italy", "Abruzzo", 42.35, 13.40),
    OriginRule(("corte dei rovi moscato", "corte dei rovi pinot grigio"), "Italy", "Veneto", 45.44, 11.00),
    OriginRule(("prosecco", "raboso", "botter"), "Italy", "Veneto", 45.44, 11.00),
    OriginRule(("astrale rosso",), "Italy", "Vino d'Italia", 42.50, 12.50, "country"),
    OriginRule(("terlaner", "terlano"), "Italy", "Alto Adige", 46.53, 11.25),
    OriginRule(("sonoma mountain", "paul hobbs"), "United States", "Sonoma County", 38.44, -122.71),
    OriginRule(("blue rock", "baby blue", "alexander valley"), "United States", "Sonoma County", 38.65, -122.85),
    OriginRule(("rhys", "bearwallow", "mt pajaro"), "United States", "California Central Coast", 36.87, -121.56),
    OriginRule(("the hilt", "santa rita hills", "bentrock", "radian vineyard"), "United States", "Santa Barbara County", 34.61, -120.08),
    OriginRule(("jonata", "the paring"), "United States", "Santa Ynez Valley", 34.61, -120.08),
    OriginRule(("daou", "perrin dobbs", "ace of spades"), "United States", "Paso Robles", 35.63, -120.69),
    OriginRule(("lapis luna cabernet sauvignon",), "United States", "Lodi", 38.13, -121.27),
    OriginRule(("00 wines", "freya hermann"), "United States", "Willamette Valley", 45.22, -123.10),
    OriginRule(("silk spice", "silk and spice"), "Portugal", "Vinho de Portugal", 39.50, -8.00, "country"),
    OriginRule(("tahbilk", "nagambie lakes"), "Australia", "Victoria", -36.79, 145.15),
    OriginRule(("marlborough", "black cottage", "two rivers"), "New Zealand", "Marlborough", -41.51, 173.96),
    OriginRule(("caballo loco grand cru limari", "limari"), "Chile", "Limari Valley", -30.62, -71.20),
    OriginRule(("caballo loco grand cru apalta", "apalta"), "Chile", "Colchagua Valley", -34.61, -71.32),
    OriginRule(("caballo loco grand cru sagrada familia", "sagrada familia"), "Chile", "Lontue Valley", -35.08, -71.28),
    OriginRule(("vina valdivieso", "caballo loco"), "Chile", "Central Valley", -34.60, -70.99),
    OriginRule(("castell de sant pau", "cava"), "Spain", "Catalonia", 41.39, 2.16),
    OriginRule(("finca munoz", "felix solis"), "Spain", "Castilla-La Mancha", 39.28, -2.98),
)

_SPARKLING_MARKERS = (
    "champagne",
    "prosecco",
    "blanc de blancs",
    "blanc de noirs",
    "brut",
    "spumante",
    "zero dosage",
    "cote des bar",
    "moscato d asti",
    "moscato spumante",
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
    "moscato spumante",
)

# Colour of every grape or blend label a guess can return. A rule whose grapes do not fit the
# wine's colour is skipped, so a guess never gives a red wine white grapes or the reverse.
GRAPE_COLOURS = {
    **dict.fromkeys(
        (
            "Aglianico", "Alicante Bouschet", "Baga", "Barbera", "Cabernet Franc", "Cabernet Sauvignon", "Carmenere",
            "Dolcetto", "Freisa", "Gamay", "Grenache", "Grenache Blend", "Malbec", "Merlot", "Montepulciano",
            "Mourvedre", "Nebbiolo", "Negroamaro", "Nero d'Avola", "Petit Verdot", "Pinot Meunier", "Pinot Noir",
            "Primitivo", "Raboso", "Red Blend", "Sangiovese", "Shiraz", "Syrah", "Tempranillo", "Touriga Nacional",
            "Zinfandel",
        ),
        "red",
    ),
    **dict.fromkeys(
        (
            "Albarino", "Aligote", "Ansonica", "Chardonnay", "Chenin Blanc", "Gewurztraminer", "Glera",
            "Gruner Veltliner", "Moscato Bianco", "Muscat", "Nascetta", "Pecorino", "Pinot Bianco", "Pinot Grigio",
            "Rhone White Blend", "Riesling", "Sauvignon Blanc", "Timorasso", "Vermentino", "Viognier", "White Blend",
        ),
        "white",
    ),
    **dict.fromkeys(("Rose Blend", "Sparkling Rose Blend"), "rose"),
}
# Wine types each grape colour can make. White sparkling is often made from black grapes.
_COLOUR_FITS = {
    "red": {"Red", "Rose", "Sparkling", "Sparkling Rose"},
    "white": {"White", "Sparkling", "Orange"},
    "rose": {"Rose", "Sparkling Rose"},
}
# The usual grape where a region makes a colour almost entirely from one grape.
_REGION_GRAPES = {
    ("Burgundy", "Red"): "Pinot Noir",
    ("Burgundy", "White"): "Chardonnay",
}


def grapes_fit(grapes: str, wine_type: str | None) -> bool:
    if wine_type is None:
        return True
    return all(wine_type in _COLOUR_FITS[GRAPE_COLOURS[g.strip()]] for g in grapes.split(",") if g.strip() in GRAPE_COLOURS)


_GRAPE_RULES: tuple[GrapeRule, ...] = (
    GrapeRule(("grenache shiraz mourvedre",), "Grenache, Shiraz, Mourvedre"),
    GrapeRule(("cabernet sauvignon",), "Cabernet Sauvignon"),
    GrapeRule(("sauvignon blanc",), "Sauvignon Blanc"),
    GrapeRule(("pinot noir",), "Pinot Noir"),
    GrapeRule(("blanc de noirs",), "Pinot Noir, Pinot Meunier", source="style_inference", confidence="medium"),
    GrapeRule(("blanc de blancs",), "Chardonnay", source="style_inference", confidence="medium"),
    GrapeRule(("chardonnay",), "Chardonnay"),
    GrapeRule(("aligote",), "Aligote"),
    GrapeRule(("pinot grigio",), "Pinot Grigio"),
    GrapeRule(("pecorino",), "Pecorino"),
    GrapeRule(("viognier",), "Viognier"),
    GrapeRule(("shiraz",), "Shiraz"),
    GrapeRule(("kingpin tinto",), "Tempranillo, Shiraz, Cabernet Sauvignon", source="vivino", confidence="medium"),
    GrapeRule(("mucho mas gold",), "Tempranillo", source="vivino", confidence="medium"),
    GrapeRule(("astrale rosso", "rosso vino d italia"), "Primitivo, Sangiovese, Montepulciano, Nero d'Avola", source="producer", confidence="high"),
    GrapeRule(("montepulciano",), "Montepulciano"),
    GrapeRule(("primitivo", "quattro passi"), "Primitivo"),
    GrapeRule(("freisa",), "Freisa"),
    GrapeRule(("nero d avola",), "Nero d'Avola"),
    GrapeRule(("barbera",), "Barbera"),
    GrapeRule(("dolcetto",), "Dolcetto"),
    GrapeRule(("moscato spumante",), "Moscato Bianco", source="regional_inference", confidence="medium"),
    GrapeRule(("moscato d asti",), "Moscato Bianco", source="regional_inference", confidence="medium"),
    GrapeRule(("prosecco",), "Glera", source="regional_inference", confidence="medium"),
    GrapeRule(("raboso",), "Raboso"),
    GrapeRule(("brunello di montalcino",), "Sangiovese", source="regional_inference", confidence="medium"),
    GrapeRule(("barolo",), "Nebbiolo", source="regional_inference", confidence="high"),
    GrapeRule(("gattinara",), "Nebbiolo", source="regional_inference", confidence="medium"),
    GrapeRule(("solea",), "Nascetta", source="winery_cuvee_inference", confidence="high"),
    GrapeRule(("derthona",), "Timorasso", source="winery_cuvee_inference", confidence="high"),
    GrapeRule(("giramonte",), "Sangiovese, Merlot", source="winery_cuvee_inference", confidence="high"),
    GrapeRule(("gorgona",), "Vermentino, Ansonica", source="winery_cuvee_inference", confidence="medium"),
    GrapeRule(("tenuta perano", "perano rialzi"), "Sangiovese", source="winery_cuvee_inference", confidence="high"),
    GrapeRule(("baby blue",), "Cabernet Sauvignon", source="winery_cuvee_inference", confidence="medium"),
    GrapeRule(("guidalberto",), "Cabernet Sauvignon, Merlot", source="winery_cuvee_inference", confidence="medium"),
    GrapeRule(("le difese",), "Cabernet Sauvignon, Sangiovese", source="winery_cuvee_inference", confidence="medium"),
    GrapeRule(("terlaner classico",), "Pinot Bianco, Chardonnay, Sauvignon Blanc", source="regional_inference", confidence="medium"),
    GrapeRule(("silk spice", "silk and spice"), "Baga, Alicante Bouschet, Touriga Nacional", source="vivino", confidence="medium"),
    GrapeRule(("caballo loco grand cru limari", "grand cru limari"), "Syrah", source="vivino", confidence="high"),
    GrapeRule(("caballo loco grand cru apalta", "grand cru apalta"), "Carmenere, Cabernet Sauvignon, Syrah", source="retailer_inference", confidence="medium"),
    GrapeRule(("caballo loco grand cru sagrada familia", "sagrada familia"), "Merlot, Petit Verdot, Cabernet Sauvignon, Cabernet Franc, Malbec", source="vivino", confidence="high"),
    GrapeRule(("chateauneuf du pape", "cotes du rhone"), "Rhone White Blend", source="regional_inference", confidence="medium"),
    GrapeRule(("chateauneuf du pape", "gigondas", "cotes du rhone"), "Grenache Blend", source="regional_inference", confidence="medium"),
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


def _detect_grapes(text: str, wine_type: str | None, region: str | None) -> tuple[str | None, str | None, str | None]:
    for rule in _GRAPE_RULES:
        if any(pattern in text for pattern in rule.patterns) and grapes_fit(rule.grapes, wine_type):
            return rule.grapes, rule.source, rule.confidence
    if (region, wine_type) in _REGION_GRAPES:
        return _REGION_GRAPES[(region, wine_type)], "regional_inference", "medium"

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
    grapes, grape_source, grape_confidence = _detect_grapes(combined_text, wine_type, origin.region if origin else None)

    # Champagne catch-all: if style is Champagne and no grape rule fired,
    # fall back to the typical assemblage blend. Blanc de Blancs / Blanc de
    # Noirs are already caught above, so this only reaches generic Brut NV
    # wines where the producer name alone drove the Champagne style.
    if grapes is None and style_family == "Champagne":
        grapes = "Pinot Noir, Chardonnay, Pinot Meunier"
        grape_source = "style_inference"
        grape_confidence = "low"

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
