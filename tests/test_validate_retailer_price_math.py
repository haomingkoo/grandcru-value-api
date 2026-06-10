from scripts.validate_retailer_price_math import live_price_from_payload, product_json_url, validate_rows


def test_product_json_url_uses_product_handle() -> None:
    assert (
        product_json_url("https://grandcruwines.com/products/2022-domaine-claude-dugat-gevrey-chambertin")
        == "https://grandcruwines.com/products/2022-domaine-claude-dugat-gevrey-chambertin.json"
    )


def test_live_price_from_payload_uses_single_bottle_price() -> None:
    payload = {
        "product": {
            "title": "2022 Domaine Claude Dugat - Gevrey Chambertin",
            "handle": "2022-domaine-claude-dugat-gevrey-chambertin",
            "variants": [
                {"title": "Default Title", "price": "170.00"},
            ],
        }
    }

    live_price = live_price_from_payload(
        payload,
        "https://grandcruwines.com/products/2022-domaine-claude-dugat-gevrey-chambertin",
    )

    assert live_price is not None
    assert live_price.unit_price == 170.0
    assert live_price.variant_quantity == 1


def test_live_price_from_payload_normalizes_case_products() -> None:
    payload = {
        "product": {
            "title": "Example Wine Case of 6",
            "handle": "example-wine-case-of-6",
            "variants": [
                {"title": "Default Title", "price": "1020.00"},
            ],
        }
    }

    live_price = live_price_from_payload(payload, "https://grandcruwines.com/products/example-wine-case-of-6")

    assert live_price is not None
    assert live_price.unit_price == 170.0
    assert live_price.variant_quantity == 6


def test_validate_rows_flags_stale_bundle_total() -> None:
    rows = [
        {
            "name_plat": "2022 Domaine Claude Dugat - Gevrey Chambertin - Red - 750 ml - Standard Bottle (Bundle of 3)",
            "quantity_plat": "3",
            "price_main": "3060.00",
            "url_main": "https://grandcruwines.com/products/2022-domaine-claude-dugat-gevrey-chambertin",
        }
    ]
    payload = {
        "product": {
            "title": "2022 Domaine Claude Dugat - Gevrey Chambertin",
            "handle": "2022-domaine-claude-dugat-gevrey-chambertin",
            "variants": [{"title": "Default Title", "price": "170.00"}],
        }
    }

    issues = validate_rows(
        rows,
        fetch_product=lambda _url, _timeout: payload,
        timeout_seconds=1,
        tolerance=0.05,
    )

    assert len(issues) == 1
    assert issues[0].stored_price == 3060.0
    assert issues[0].expected_price == 510.0


def test_validate_rows_accepts_scaled_bundle_total() -> None:
    rows = [
        {
            "name_plat": "2022 Domaine Claude Dugat - Gevrey Chambertin - Red - 750 ml - Standard Bottle (Bundle of 3)",
            "quantity_plat": "3",
            "price_main": "510.00",
            "url_main": "https://grandcruwines.com/products/2022-domaine-claude-dugat-gevrey-chambertin",
        }
    ]
    payload = {
        "product": {
            "title": "2022 Domaine Claude Dugat - Gevrey Chambertin",
            "handle": "2022-domaine-claude-dugat-gevrey-chambertin",
            "variants": [{"title": "Default Title", "price": "170.00"}],
        }
    }

    issues = validate_rows(
        rows,
        fetch_product=lambda _url, _timeout: payload,
        timeout_seconds=1,
        tolerance=0.05,
    )

    assert issues == []
