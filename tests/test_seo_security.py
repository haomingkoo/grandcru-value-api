from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_frontend_sets_security_headers() -> None:
    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["referrer-policy"] == "strict-origin-when-cross-origin"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
    assert "camera=()" in response.headers["permissions-policy"]


def test_robots_txt_allows_public_site_and_blocks_ops() -> None:
    response = client.get("/robots.txt")

    assert response.status_code == 200
    body = response.text
    assert "Allow: /" in body
    assert "Disallow: /ops/" in body
    assert "Sitemap: https://wine.kooexperience.com/sitemap.xml" in body


def test_sitemap_xml_lists_canonical_pages() -> None:
    response = client.get("/sitemap.xml")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/xml")
    body = response.text
    assert "<loc>https://wine.kooexperience.com/</loc>" in body
    assert "<loc>https://wine.kooexperience.com/legal</loc>" in body
