"""Shared allowlists for wine data quality checks.

These are intentionally small and documented. Adding an entry here means we
accept a known data gap until the correct Vivino page or SGD price is found.
"""

from __future__ import annotations

# No Vivino SG page found for this wine yet.
# The override row intentionally has no URL to block a wrong fuzzy match to
# Philippe Girard Puligny-Montrachet (different producer).
# To close: find the correct La Croix de Brully Vivino URL and add a full override.
WINES_MISSING_VIVINO_URL: set[str] = {
    "2022 La Croix de Brully - Puligny-Montrachet Les Enseignères - White - 750 ml - Standard Bottle",
}

# Suppression overrides: rows in vivino_overrides.csv with no URL or rating
# that intentionally block wrong fuzzy matches. These entries get downgraded
# to "none" by the import guard in import_wine_data.py.
# Format: match_name -> reason.
SUPPRESSION_OVERRIDES: dict[str, str] = {
    "2022 La Croix de Brully - Puligny-Montrachet Les Enseignères - White - 750 ml - Standard Bottle":
        "Blocks wrong fuzzy match to Philippe Girard; add correct URL when found",
    "2023 Hudelot - Baillet - Bourgogne Hautes Cotes de Nuits Rouge - Red - 750 ml - Standard Bottle":
        "Pre-emptive suppressor: not currently in comparison, prevents wrong Blanc fuzzy match",
}

# Vivino prices are fetched from the SG page, which can be blocked from hosted
# datacenter IPs. Prices for these wines must be entered manually in
# vivino_overrides.csv when a reliable SGD price is available.
# Wine names must match wine_deals.wine_name exactly, including Bundle suffixes.
WINES_MISSING_VIVINO_PRICE: set[str] = {
    "2014 Rhys - Chardonnay Bearwallow Vineyard - White - 1.5 L - Magnum",
    "2017 Paul Hobbs - Chardonnay Dinner Vineyard Cuvee Agustina Sonoma Mountain - White - 750 ml - Standard Bottle",
    "2017 Rhys - Chardonnay Bearwallow Vineyard - White - 1.5 L - Magnum",
    "2017 Rhys - Chardonnay Mt. Pajaro Vineyard - White - 1.5 L - Magnum",
    "2018 Louise Brison - a L'Aube de Côte des Bar Millésime - White - 750 ml - Standard Bottle",
    "2021 Chateau Tahbilk - Shiraz Tower Release - Red - 750 ml - Standard Bottle",
    "2021 Hudelot - Baillet - Bonnes Mares - Red - 750 ml - Standard Bottle (Bundle of 3)",
    "2021 Pierre Boisson - Auxey Duresses - White - 750 ml - Standard Bottle (Bundle of 6)",
    "2020 Frescobaldi - Gorgona - Red - 750 ml - Standard Bottle",
    "2020 The Hilt - Chardonnay Estate Santa Rita Hills - White - 750 ml - Standard Bottle",
    "2022 00 Wines - Freya Hermann Cuvee Chardonnay - White - 750 ml - Standard Bottle",
    "2022 Domaine Claude Dugat - La Gibryotte Charmes Chambertin Grand Cru - Red - 750 ml - Standard Bottle (Bundle of 3)",
    "2022 La Croix de Brully - Puligny-Montrachet Les Enseignères - White - 750 ml - Standard Bottle",
    "2022 The Hilt - Chardonnay Estate Santa Rita Hills - White - 750 ml - Standard Bottle",
    "2023 Hudelot - Baillet - Chambolle Musigny Charmes - Red - 750 ml - Standard Bottle (Bundle of 3)",
    "NV Adrien Renoir - Grand Cru Le Terroir - White - 750 ml - Standard Bottle",
    "NV Botter - Brilla Asolo Prosecco Superiore DOCG - White - 750 ml - Standard Bottle",
    "NV Castell de Sant Pau - Brut Rose - Rose - 750 ml - Standard Bottle",
    "NV Charles Heidsieck - Brut Reserve - White - 750 ml - Standard Bottle",
    "NV Coutier - Tradition Brut - White - 750 ml - Standard Bottle",
    "NV Dhondt-Grellet - Extra Brut Blanc de Blancs Premier Cru Les Terres Fines (Base 2022) - White - 750 ml - Standard Bottle",
    "NV Felix Solis - Mucho Mas Gold - Red - 750 ml - Standard Bottle",
    "NV Miraval - Fleur De Miraval Exclusivement Rose 3 - Rose - 750 ml - Standard Bottle",
    "NV Miraval - Petite Fleur (Base 2020) - Rose - 750 ml - Standard Bottle",
}
