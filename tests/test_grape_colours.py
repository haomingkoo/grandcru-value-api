import pytest

from app.wine_metadata import _GRAPE_RULES, GRAPE_COLOURS, derive_wine_metadata


def grapes_of(name):
    return derive_wine_metadata(wine_name=name, quantity=1, volume="750ml").grapes


@pytest.mark.parametrize(
    ("name", "grapes"),
    [
        # Red wines from white-wine villages. Seen live tagged Chardonnay.
        ("2019 Chateau de la Maltroye - Chassagne Montrachet Boudriottes Rouge - Red - 750 ml - Standard Bottle", "Pinot Noir"),
        ("2022 Henri et Gilles Buisson - Saint-Romain Sous Roche - Red - 750 ml - Standard Bottle", "Pinot Noir"),
        ("2021 Domaine Xavier Monnot - Meursault Charmes - White - 750 ml - Standard Bottle", "Chardonnay"),
        ("2022 Domaine Claude Dugat - Gevrey Chambertin - Red - 750 ml - Standard Bottle", "Pinot Noir"),
        # A Burgundy producer we know, at a village no rule names.
        ("2022 Pierre Boisson - Pommard - Red - 750 ml - Standard Bottle", "Pinot Noir"),
        ("2022 Pierre Boisson - Bourgogne Blanc - White - 750 ml - Standard Bottle", "Chardonnay"),
        # A named grape still wins over the region's usual grape.
        ("2022 Pierre Boisson - Bourgogne Aligote - White - 750 ml - Standard Bottle", "Aligote"),
        # Rhone: the same appellation in both colours.
        ("2022 Chateau Beaucastel - Coudoulet de Beaucastel Cotes du Rhone - Red - 750 ml - Standard Bottle", "Grenache Blend"),
        ("2024 Famille Perrin - Cotes du Rhone Reserve Blanc - White - 750 ml - Standard Bottle", "Rhone White Blend"),
        ("2021 Chateau Beaucastel - Chateauneuf du Pape Blanc - White - 750 ml - Standard Bottle", "Rhone White Blend"),
        ("2021 Chateau Beaucastel - Chateauneuf du Pape - Red - 750 ml - Standard Bottle", "Grenache Blend"),
        # A producer rule that only fits one colour leaves the other colour unguessed.
        ("2023 Silk & Spice - Red Blend - Red - 750 ml - Standard Bottle", "Baga, Alicante Bouschet, Touriga Nacional"),
        ("2024 Silk & Spice - White Blend - White - 750 ml - Standard Bottle", None),
        # Appellations whose rules fix the grapes.
        ("2021 Feudo Italia - Amarone della Valpolicella DOCG - Red - 750 ml - Standard Bottle", "Corvina Blend"),
        ("2018 Tommaso Bussola - Valpolicella Classico Superiore TB - Red - 750 ml - Standard Bottle", "Corvina Blend"),
        ("2020 Tommaso Bussola - Valpolicella Superiore Ripasso Ca' del Laito - Red - 750 ml - Standard Bottle", "Corvina Blend"),
        ("2022 Stonyridge Vineyard - Blossom Bordeaux Blend - Red - 750 ml - Standard Bottle", "Bordeaux Blend"),
        # Sparkling whites from black grapes still fit.
        ("NV Coutier - Blanc de Noirs Grand Cru - White - 750 ml - Standard Bottle", "Pinot Noir, Pinot Meunier"),
    ],
)
def test_grape_guess_fits_the_wine_colour(name, grapes):
    assert grapes_of(name) == grapes


def test_every_grape_a_rule_can_return_has_a_colour():
    # Without a colour the check cannot skip a wrong rule, so a new rule must add its grapes here.
    for rule in _GRAPE_RULES:
        for grape in rule.grapes.split(","):
            assert grape.strip() in GRAPE_COLOURS, f"{grape.strip()} (rule {rule.patterns[0]!r}) needs a colour"
