"""One-line wine notes: the grape, and how that grape usually tastes.

Notes are written per grape, not per wine, and say what is typical. Nothing comes from
Vivino reviews or retailer copy. A grape that does not fit the wine's colour is treated
as a wrong guess and gets no line; a guessed grape reads "Likely".
"""

from __future__ import annotations

from typing import Any

from app.wine_metadata import GRAPE_COLOURS

# Typical style of a still wine from each grape; its colour comes from GRAPE_COLOURS.
GRAPE_NOTES = {
    "Aglianico": "Firm tannins, high acidity and dark fruit; southern Italy's grape for ageing.",
    "Albarino": "Crisp and aromatic, with stone fruit and a salty edge.",
    "Barbera": "Juicy and high in acidity, low in tannin, with dark cherry.",
    "Cabernet Franc": "Medium body, red fruit and a leafy, herbal note.",
    "Cabernet Sauvignon": "Full body, blackcurrant and firm tannins; often ages well.",
    "Carmenere": "Dark fruit with a green-pepper note; Chile's signature grape.",
    "Chardonnay": "Crisp and citrusy, or rich and buttery when aged in oak.",
    "Chenin Blanc": "High acidity, apple and honey; made from dry to sweet.",
    "Corvina": "Cherry and bright acidity; Amarone dries it first for a richer, stronger wine.",
    "Dolcetto": "Soft, dark-fruited and low in acidity, with a bitter-almond finish.",
    "Freisa": "Piedmont red with raspberry and firm tannins.",
    "Gewurztraminer": "Very aromatic, with lychee, rose and spice; low acidity.",
    "Grenache": "Juicy red fruit and spice, with gentle tannins.",
    "Gruner Veltliner": "Crisp, with citrus and a white-pepper note; Austria's grape.",
    "Merlot": "Soft and plummy, rounder than Cabernet.",
    "Montepulciano": "Soft, plummy dark fruit and easy tannins.",
    "Muscat": "Grapey and floral; often off-dry or sweet.",
    "Nascetta": "Rare Piedmont white, aromatic with citrus and herbs.",
    "Nebbiolo": "Pale but high in tannin and acidity; Barolo's grape, built to age.",
    "Negroamaro": "Dark, rustic fruit with a bitter-cherry edge; from Puglia.",
    "Pinot Grigio": "Light and crisp, with pear and citrus.",
    "Pinot Noir": "Light to medium body, red fruit and soft tannins.",
    "Primitivo": "Ripe, jammy dark fruit and soft tannins; the same grape as Zinfandel.",
    "Riesling": "High acidity, citrus and stone fruit; made from dry to sweet.",
    "Sangiovese": "Bright acidity, sour cherry and a savoury finish; Chianti's grape.",
    "Sauvignon Blanc": "Zesty and aromatic, with citrus and green notes.",
    "Shiraz": "Dark fruit and spice, usually riper and fuller than Syrah.",
    "Syrah": "Dark fruit, black pepper and a firm structure.",
    "Tempranillo": "Medium body, cherry and leather; Rioja's main grape.",
    "Timorasso": "Full-bodied Piedmont white with a mineral edge; ages well.",
    "Vermentino": "Fresh and zesty, with citrus and a bitter-herb finish.",
    "Viognier": "Full and perfumed, with apricot, peach and flowers.",
    "Zinfandel": "Ripe, jammy dark fruit, often high in alcohol; the same grape as Primitivo.",
}

# Sparkling wines, by grape mix (sorted).
SPARKLING_NOTES = {
    ("Chardonnay",): "Blanc de blancs: all Chardonnay, usually crisp and citrusy.",
    ("Glera",): "Prosecco's grape: light, fruity bubbles with pear and apple.",
    ("Pinot Meunier", "Pinot Noir"): "Blanc de noirs: white bubbles from black grapes, usually fuller.",
    ("Chardonnay", "Pinot Meunier", "Pinot Noir"): "The classic Champagne blend.",
}

# Catch-all labels from the metadata guesses; they add nothing to the row.
GENERIC_BLENDS = {"Red Blend", "White Blend", "Rose Blend", "Sparkling Rose Blend", "Rhone White Blend"}

FITS = {
    "red": {"Red", "Rose", "Sparkling", "Champagne"},
    "white": {"White", "Sparkling", "Champagne", "Orange"},
    "rose": {"Rose", "Sparkling"},
}
STILL = {"red": "Red", "white": "White", "rose": "Rose"}
SPARKLING = {"Sparkling", "Champagne"}


def _join(names: list[str]) -> str:
    return names[0] if len(names) == 1 else f"{', '.join(names[:-1])} and {names[-1]}"


def wine_note(deal: Any) -> str | None:
    grapes = (getattr(deal, "grapes", None) or "").strip()
    style = getattr(deal, "style_family", None)
    if not grapes or not style or grapes in GENERIC_BLENDS:
        return None
    confidence = getattr(deal, "grape_confidence", None)
    if confidence == "low":
        return None

    blend = grapes.endswith(" Blend")
    names = [grapes.removesuffix(" Blend")] if blend else [g.strip() for g in grapes.split(",") if g.strip()]
    if any(name in GRAPE_COLOURS and style not in FITS[GRAPE_COLOURS[name]] for name in names):
        return None

    label = f"{names[0]} blend" if blend else _join(names)
    if style in SPARKLING:
        text = SPARKLING_NOTES.get(tuple(sorted(names))) if not blend else None
    elif len(names) == 1 and names[0] in GRAPE_NOTES and STILL[GRAPE_COLOURS[names[0]]] == style:
        text = GRAPE_NOTES[names[0]]
    else:
        text = None

    guessed = (getattr(deal, "grape_source", None) or "").endswith("inference")
    return f"{'Likely ' if guessed else ''}{label}.{f' {text}' if text else ''}"
