from __future__ import annotations

from datetime import UTC, datetime
import socket
from threading import Thread
import time

import httpx
import pytest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import uvicorn

from app.database import Base, get_session
from app.main import app
from app.models import IngestionRun, WineDeal


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture()
def browser_app_url(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'browser.db'}",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    with Session() as session:
        session.add(
            IngestionRun(
                started_at=datetime.now(UTC),
                finished_at=datetime.now(UTC),
                status="success",
                comparison_rows=2,
                vivino_rows=2,
                merged_rows=2,
            )
        )
        session.add_all(
            [
                WineDeal(
                    wine_name="2023 Visible Price Burgundy",
                    platinum_url="https://example.com/platinum",
                    grand_cru_url="https://example.com/grand-cru",
                    vivino_url="https://example.com/vivino",
                    price_platinum=120.0,
                    price_grand_cru=150.0,
                    price_diff=-30.0,
                    price_diff_pct=-20.0,
                    cheaper_side="Platinum Cheaper",
                    platinum_in_stock=True,
                    grand_cru_in_stock=True,
                    vivino_price=180.0,
                    vivino_rating=4.2,
                    vivino_num_ratings=1234,
                    vivino_description="Regions · Burgundy Red · Cherry, raspberry, and clean fruit.",
                    deal_score=61.0,
                    country="France",
                    region="Burgundy",
                    wine_type="Red",
                    style_family="Red",
                    grapes="Pinot Noir",
                    offering_type="Single Bottle",
                    producer="Visible Estate",
                    volume="750ml",
                    quantity=1,
                ),
                WineDeal(
                    wine_name="2022 Market Only White",
                    platinum_url="https://example.com/market-only",
                    price_platinum=90.0,
                    price_grand_cru=None,
                    price_diff=None,
                    price_diff_pct=None,
                    cheaper_side="No Match",
                    vivino_price=100.0,
                    vivino_rating=4.0,
                    vivino_num_ratings=250,
                    deal_score=35.0,
                    country="Italy",
                    region="Piedmont",
                    wine_type="White",
                    style_family="White",
                    grapes="Timorasso",
                    offering_type="Single Bottle",
                    producer="Market Estate",
                    volume="750ml",
                    quantity=1,
                ),
            ]
        )
        session.commit()

    def override_session():
        session = Session()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_session] = override_session
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            access_log=False,
        )
    )
    thread = Thread(target=server.run, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"

    try:
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                if httpx.get(f"{base_url}/health", timeout=1).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.1)
        else:
            raise RuntimeError("Timed out waiting for browser test server")

        yield base_url
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        app.dependency_overrides.pop(get_session, None)
        engine.dispose()


@pytest.fixture()
def mobile_browser():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=390,844")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)
    try:
        yield driver
    finally:
        driver.quit()


def test_mobile_all_offers_shows_prices_before_actions(browser_app_url, mobile_browser) -> None:
    mobile_browser.get(f"{browser_app_url}/?e2e={time.time_ns()}")

    section_select = WebDriverWait(mobile_browser, 10).until(
        lambda driver: driver.find_element(By.ID, "sectionSelect")
    )
    Select(section_select).select_by_value("offersSection")

    first_row = WebDriverWait(mobile_browser, 10).until(
        lambda driver: driver.find_element(By.CSS_SELECTOR, ".deal-row")
    )
    price_strip = first_row.find_element(By.CSS_SELECTOR, ".mobile-price-strip")
    action_links = first_row.find_element(By.CSS_SELECTOR, ".wine-links")
    detailed_price_cell = first_row.find_elements(By.TAG_NAME, "td")[3]

    strip_text = price_strip.text
    assert "PLATINUM" in strip_text
    assert "$120.00" in strip_text
    assert "GRAND CRU" in strip_text
    assert "$150.00" in strip_text
    assert "Platinum -20.0%" in strip_text
    assert price_strip.value_of_css_property("display") == "grid"

    metrics = mobile_browser.execute_script(
        """
        const row = arguments[0]
        const strip = arguments[1]
        const links = arguments[2]
        const detailedPrice = arguments[3]
        return {
          activeSections: [...document.querySelectorAll('.browse-section.is-active')].map((el) => el.id),
          rowTop: row.getBoundingClientRect().top,
          stripTop: strip.getBoundingClientRect().top,
          stripWidth: strip.getBoundingClientRect().width,
          linksTop: links.getBoundingClientRect().top,
          detailedPriceTop: detailedPrice.getBoundingClientRect().top,
          scrollWidth: document.documentElement.scrollWidth,
          innerWidth: window.innerWidth,
        }
        """,
        first_row,
        price_strip,
        action_links,
        detailed_price_cell,
    )

    assert metrics["activeSections"] == ["offersSection"]
    assert metrics["stripTop"] > metrics["rowTop"]
    assert metrics["stripTop"] < metrics["linksTop"]
    assert metrics["stripTop"] < metrics["detailedPriceTop"]
    assert metrics["stripWidth"] >= 240
    assert metrics["scrollWidth"] <= metrics["innerWidth"]


def test_filter_cards_activate_visible_offers_table(browser_app_url, mobile_browser) -> None:
    mobile_browser.get(f"{browser_app_url}/?e2e={time.time_ns()}")

    section_select = WebDriverWait(mobile_browser, 10).until(
        lambda driver: driver.find_element(By.ID, "sectionSelect")
    )
    Select(section_select).select_by_value("placeSection")

    country_button = WebDriverWait(mobile_browser, 10).until(
        lambda driver: driver.find_element(By.CSS_SELECTOR, "[data-country-pick='France']")
    )
    mobile_browser.execute_script("arguments[0].click()", country_button)

    WebDriverWait(mobile_browser, 10).until(
        lambda driver: driver.execute_script(
            "return document.querySelector('#offersSection')?.classList.contains('is-active') === true"
        )
    )

    metrics = mobile_browser.execute_script(
        """
        const offers = document.querySelector('#offersSection')
        const place = document.querySelector('#placeSection')
        const row = document.querySelector('.deal-row')
        const strip = row?.querySelector('.mobile-price-strip')
        return {
          activeSections: [...document.querySelectorAll('.browse-section.is-active')].map((el) => el.id),
          sectionValue: document.querySelector('#sectionSelect')?.value,
          offersDisplay: getComputedStyle(offers).display,
          placeDisplay: getComputedStyle(place).display,
          rowCount: document.querySelectorAll('.deal-row').length,
          stripText: strip?.innerText || '',
          urlHash: window.location.hash,
        }
        """
    )

    assert metrics["activeSections"] == ["offersSection"]
    assert metrics["sectionValue"] == "offersSection"
    assert metrics["offersDisplay"] != "none"
    assert metrics["placeDisplay"] == "none"
    assert metrics["rowCount"] >= 1
    assert "$120.00" in metrics["stripText"]
    assert metrics["urlHash"] == "#offersSection"
