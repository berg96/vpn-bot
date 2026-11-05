"""Лендинг: радар занимает колонку, подсветка платформы без золотого градиента."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CSS = (ROOT / "landing" / "static" / "landing.css").read_text(encoding="utf-8")
JS = (ROOT / "landing" / "static" / "app.js").read_text(encoding="utf-8")


def _rule(selector: str) -> str:
    start = CSS.index(selector + "{")
    return CSS[start : CSS.index("}", start)]


def test_radar_has_a_definite_width():
    rule = _rule(".radar")
    assert "width:min(100%,520px)" in rule
    assert "justify-self:end" in rule
    assert "align-self:center" in rule


def test_platform_highlight_stays_on_the_neutral_surface():
    assert "#c97f00" not in CSS
    highlight = _rule(".plat-card.highlight")
    assert "linear-gradient" not in highlight
    assert "scale(" not in highlight
    assert "box-shadow" not in highlight
    assert "0 16px 44px" not in CSS


def test_labels_are_not_tracked_uppercase():
    assert "text-transform:uppercase" not in CSS
    assert "letter-spacing:.08em" not in CSS
    assert "letter-spacing:.1em" not in CSS


def test_status_dot_and_blips_have_no_halo():
    assert "box-shadow" not in _rule(".eyebrow .dot")
    assert "box-shadow" not in _rule(".blip")
    assert "@keyframes pulse" not in CSS


def test_recommend_badge_is_plain_text():
    assert "★" not in JS
    assert "Рекомендуем" in JS
