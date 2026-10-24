"""SEO-разметка лендинга: robots/sitemap/llms и валидность JSON-LD на страницах.

Шаблоны рендерим напрямую через Jinja, без приложения: тест не тянет fastapi
и слоу-апи и ловит главное: условные поля в JSON-LD (ИНН, ОГРНИП, адрес) не
должны давать битый JSON, когда реквизитов в окружении нет.
"""

import json
import os
import re
import sys
import xml.etree.ElementTree as ET

import pytest
from jinja2 import Environment, FileSystemLoader

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from landing import seo  # noqa: E402

SITE = "https://example.test"
TPL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "landing", "templates")

PLANS = [
    {"name": "1 месяц", "price": 250},
    {"name": "3 месяца", "price": 599},
    {"name": "1 год", "price": 1890},
]
PRICES = {"1m": 250, "3m": 599, "1y": 1890}
TRIAL_HOURS = 3


class _URL:
    def __init__(self, path):
        self.path = path
        self.query = ""


class _Request:
    def __init__(self, path):
        self.url = _URL(path)


def _env():
    return Environment(loader=FileSystemLoader(TPL_DIR))


def _base_context(path, with_requisites=True):
    ctx = {
        "request": _Request(path),
        "site_url": SITE,
        "bot_username": "radarshield_bot",
        "support_username": "radarshield_support_bot",
        "support_email": "support@example.test",
        "support_phone": "",
        "ip_full_name": "ИП Тестов Тест Тестович",
        "ip_inn": "123456789012" if with_requisites else "",
        "ip_ogrnip": "321456789012345" if with_requisites else "",
        "ip_address": "Калининград" if with_requisites else "",
        "updated_at": "23 апреля 2026 г.",
        "turnstile_site_key": "",
        "downloads": {},
        "trial_hours": 3,
        "active_token": None,
        "link_uid": "",
        "link_sig": "",
        "home_faq": seo.home_faq(PRICES, TRIAL_HOURS),
        "home_jsonld": seo.home_jsonld(SITE, PLANS, seo.home_faq(PRICES, TRIAL_HOURS)),
        "page_updated": "2026-10-24",
        "title": "Публичная оферта",
        "heading": "Публичная оферта (договор)",
        "description": "Описание страницы",
    }
    return ctx


def _jsonld_blocks(html):
    return re.findall(
        r'<script type="application/ld\+json">(.*?)</script>', html, re.S)


@pytest.mark.parametrize("tpl,path", [
    ("index.html", "/"),
    ("guide.html", "/guide"),
    ("offer.html", "/offer"),
    ("privacy.html", "/privacy"),
    ("refund.html", "/refund"),
    ("security.html", "/security"),
])
@pytest.mark.parametrize("with_requisites", [True, False])
def test_jsonld_valid(tpl, path, with_requisites):
    html = _env().get_template(tpl).render(**_base_context(path, with_requisites))
    blocks = _jsonld_blocks(html)
    assert blocks, f"{tpl}: нет ни одного блока JSON-LD"
    for raw in blocks:
        json.loads(raw)


def test_canonical_and_og_use_site_url():
    html = _env().get_template("index.html").render(**_base_context("/"))
    assert f'<link rel="canonical" href="{SITE}/">' in html
    assert f'content="{SITE}/static/og-image.png"' in html
    assert "og-image.svg" not in html


def test_home_faq_visible_and_matches_markup():
    html = _env().get_template("index.html").render(**_base_context("/"))
    graph = json.loads(_jsonld_blocks(html)[-1])["@graph"]
    faq = next(n for n in graph if n["@type"] == "FAQPage")
    assert len(faq["mainEntity"]) == len(seo._HOME_FAQ)
    for question, answer in seo.home_faq(PRICES, TRIAL_HOURS):
        assert question in html
        assert answer in html


def test_home_jsonld_offers_from_plans():
    graph = seo.home_jsonld(SITE, PLANS, seo.home_faq(PRICES, TRIAL_HOURS))["@graph"]
    app = next(n for n in graph if n["@type"] == "SoftwareApplication")
    prices = [o["price"] for o in app["offers"]]
    assert prices == ["250", "599", "1890", "0"]


def test_robots_opens_site_and_closes_private():
    txt = seo.robots_txt(SITE)
    assert f"Sitemap: {SITE}/sitemap.xml" in txt
    assert "User-agent: GPTBot" in txt
    for private in ("/sub/", "/api/", "/pay", "/trial"):
        assert f"Disallow: {private}" in txt


def test_sitemap_lists_public_pages_with_lastmod():
    root = ET.fromstring(seo.sitemap_xml(SITE))
    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locs = [u.findtext("s:loc", namespaces=ns) for u in root.findall("s:url", ns)]
    assert locs == [f"{SITE}{p}" for p, _, _, _ in seo.PAGES]
    for url in root.findall("s:url", ns):
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}",
                            url.findtext("s:lastmod", namespaces=ns))


def test_llms_txt_has_facts_and_links():
    txt = seo.llms_txt(SITE, "radarshield_bot", "support@example.test",
                       PRICES, TRIAL_HOURS)
    assert txt.startswith("# RadarShield")
    assert f"{SITE}/guide" in txt
    assert "1890 ₽" in txt
    assert "https://t.me/radarshield_bot" in txt


def test_texts_follow_real_tariffs():
    """Цены в FAQ и llms.txt берутся из конфига, а не из прошлогодней правки."""
    import config as cfg
    prices = {k: kop // 100 for k, (_n, _d, _s, _ss, kop, _rs) in cfg.PLANS.items()}
    answers = " ".join(a for _q, a in seo.home_faq(prices, TRIAL_HOURS))
    txt = seo.llms_txt(SITE, "bot", "mail@example.test", prices, TRIAL_HOURS)
    for value in prices.values():
        assert f"{value} ₽" in answers
        assert f"{value} ₽" in txt
