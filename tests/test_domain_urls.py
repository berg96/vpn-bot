"""Новые ссылки подписки/бота смотрят на radarshield.net, не на mooo.com."""

from pathlib import Path

import bot
import sub_tokens

ROOT = Path(__file__).resolve().parent.parent


def test_sub_tokens_default_base_is_net():
    url = sub_tokens.sub_url(1_000_001)
    assert url.startswith("https://radarshield.net/sub/")
    assert url.endswith(f"#{sub_tokens.PROFILE_NAME}")
    assert "mooo.com" not in url


def test_bot_landing_urls_use_net():
    assert bot.LANDING_BASE_URL.rstrip("/") == "https://radarshield.net"
    assert bot._stable_sub_url(42).startswith("https://radarshield.net/sub/")
    assert "mooo.com" not in bot._pay_url(42)
    assert "radarshield.net/refund" in bot.REFUND_NOTE


def test_landing_app_defaults_to_net():
    src = (ROOT / "landing" / "app.py").read_text(encoding="utf-8")
    assert 'SITE_URL = os.environ.get("SITE_URL", "https://radarshield.net")' in src
    assert "DOMAIN-SUFFIX,{SITE_HOST},DIRECT" in src
    assert "DOMAIN-SUFFIX,{LEGACY_SITE_HOST},DIRECT" in src
    # Origin-пин ломает доступ через CF у провайдеров с TCP-блоком IP мастера.
    assert "radarshield.mooo.com: 82.38.171.220" not in src
    assert "hosts:\n  radarshield" not in src


def test_domain_move_campaign_in_runner():
    src = (ROOT / "vpn-campaign.py").read_text(encoding="utf-8")
    assert 'name="domain_move"' in src
    assert "DOMAIN_MOVE" in src
    assert "get_reachable_users" in src
    assert "Мы переехали на radarshield.net" in src
    assert "Сайт RadarShield теперь на radarshield.net" in src
    assert "красная кнопка справа на карточке" in src
    assert "на котором работает наш VPN" in src
    assert "Удалите" in src
    assert "_domain_move_has_sub" in src
    assert "_domain_move_buttons" in src
    assert "Открыть в приложении" in src
    assert "mooo.com" not in _domain_move_user_facing_snippets(src)
    assert 'SITE_URL", "https://radarshield.net"' in src


def _domain_move_user_facing_snippets(src: str) -> str:
    """Только тексты рассылки domain_move, без комментариев про legacy mooo."""
    start = src.index("_DOMAIN_MOVE_HOWTO")
    end = src.index("DOMAIN_MOVE = campaigns.Campaign")
    return src[start:end]


def test_dynamic_seo_defaults_to_net():
    """robots/sitemap из landing.seo от SITE_URL; статики в static/ больше нет."""
    from landing import seo

    assert not (ROOT / "landing" / "static" / "robots.txt").exists()
    assert not (ROOT / "landing" / "static" / "sitemap.xml").exists()
    site = "https://radarshield.net"
    robots = seo.robots_txt(site)
    sitemap = seo.sitemap_xml(site)
    assert f"Sitemap: {site}/sitemap.xml" in robots
    assert f"<loc>{site}/</loc>" in sitemap
    assert f"<loc>{site}/dlya-rossii</loc>" in sitemap
    assert "mooo.com" not in robots
    assert "mooo.com" not in sitemap
