"""robots.txt, sitemap.xml и llms.txt генерируются из SITE_URL и mtime шаблонов,
чтобы при смене домена и правке страниц не разъезжались с реальностью."""

import os
from datetime import datetime, timezone

_TPL_DIR = os.path.join(os.path.dirname(__file__), "templates")

# (путь, файл шаблона, changefreq, priority)
PAGES = [
    ("/", "index.html", "weekly", "1.0"),
    ("/guide", "guide.html", "monthly", "0.9"),
    ("/offer", "offer.html", "yearly", "0.3"),
    ("/privacy", "privacy.html", "yearly", "0.3"),
    ("/refund", "refund.html", "yearly", "0.3"),
    ("/security", "security.html", "yearly", "0.3"),
]

# ИИ-краулеры перечислены явно: дефолтный «Allow: /» их и так пускает, но при
# любой будущей правке robots видно, что доступ для них осознанный.
_AI_BOTS = [
    "GPTBot", "OAI-SearchBot", "ChatGPT-User",
    "ClaudeBot", "Claude-SearchBot", "Claude-User",
    "PerplexityBot", "Perplexity-User",
    "Google-Extended", "Applebot-Extended", "Bingbot", "YandexBot",
]

_PRIVATE = ["/sub/", "/open/", "/s/", "/i/", "/api/", "/pay", "/trial"]


def page_lastmod(tpl: str) -> str:
    path = os.path.join(_TPL_DIR, tpl)
    try:
        ts = os.path.getmtime(path)
    except OSError:
        ts = datetime.now(timezone.utc).timestamp()
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def robots_txt(site_url: str) -> str:
    lines = [
        "User-agent: *",
        "Allow: /",
        "",
        "# Приватные ссылки: подписки, персональные страницы, оплата, API.",
        "# В индексе им делать нечего: токены не должны утекать в выдачу.",
    ]
    lines += [f"Disallow: {p}" for p in _PRIVATE]
    lines += ["", "# ИИ-краулеры: доступ открыт, приватные пути закрыты так же."]
    for bot in _AI_BOTS:
        lines += [f"User-agent: {bot}", "Allow: /"]
        lines += [f"Disallow: {p}" for p in _PRIVATE]
        lines.append("")
    lines.append(f"Sitemap: {site_url}/sitemap.xml")
    return "\n".join(lines) + "\n"


def sitemap_xml(site_url: str) -> str:
    out = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    for path, tpl, freq, prio in PAGES:
        out += ["  <url>",
                f"    <loc>{site_url}{path}</loc>",
                f"    <lastmod>{page_lastmod(tpl)}</lastmod>",
                f"    <changefreq>{freq}</changefreq>",
                f"    <priority>{prio}</priority>",
                "  </url>"]
    out.append("</urlset>")
    return "\n".join(out) + "\n"


# Вопросы главной: один источник и для видимого блока, и для FAQPage.
# Разметка без видимого текста считается нарушением, а дублировать тексты
# руками значит рано или поздно их разъехать. Ответы короткие (40-60 слов):
# ИИ-движки цитируют такие целиком. Цены и часы пробного подставляются из
# config.PLANS и LANDING_TRIAL_HOURS, иначе тексты переживут смену тарифов.
_HOME_FAQ = [
    (
        "Будет ли работать, если провайдер режет VPN?",
        "Для этого в сервисе два разных транспорта. Основной, VLESS Reality, "
        "выглядит для оборудования провайдера как обычный HTTPS-визит на "
        "популярный сайт. Если его всё же перекрыли, в том же профиле есть "
        "Hysteria2 поверх UDP, он проходит там, где закрыли обычные каналы. "
        "Переключение делается одним нажатием в приложении.",
    ),
    (
        "Российские сайты и банки не сломаются?",
        "Нет: правила маршрутизации встроены в профиль. Яндекс, VK, Сбер, Ozon, "
        "Госуслуги и другие российские ресурсы идут напрямую с вашего домашнего "
        "адреса, минуя наши серверы. Банк видит обычный вход из России и не "
        "блокирует операции, а скорость на этих сайтах не падает.",
    ),
    (
        "На каких устройствах работает одна подписка?",
        "На Android, iPhone, iPad, Windows и macOS. Ссылка подписки одна и та же "
        "для всех устройств: на Android и Windows ставится FLClash, на технике "
        "Apple ставится Karing из App Store. Пошаговая установка с картинками есть "
        "в инструкции на сайте.",
    ),
    (
        "Сколько стоит и можно ли проверить бесплатно?",
        "Месяц стоит {m} ₽, три месяца {q} ₽, год {y} ₽, то есть {pm} ₽ в месяц. "
        "Трафик в любом тарифе без лимита. Проверить можно бесплатно: {h} часа "
        "прямо с сайта без регистрации и карты, плюс 7 дней и 5 ГБ через "
        "Telegram-бота.",
    ),
    (
        "Что вы храните о подключениях?",
        "Только технические данные: объём трафика и время последнего "
        "подключения: они нужны для лимитов тарифа и поддержки. Содержимое "
        "трафика, адреса сайтов и историю посещений мы не логируем и не "
        "анализируем. Подробности есть в политике конфиденциальности.",
    ),
    (
        "Можно вернуть деньги, если не подошло?",
        "Да. В первые 24 часа после оплаты, если подпиской не пользовались, "
        "деньги возвращаются полностью. Позже возврат считается за "
        "неиспользованные дни: из суммы вычитаются уже потраченные. Заявку "
        "принимаем в Telegram-поддержке и на почте, условия есть на странице возврата.",
    ),
]


def home_faq(prices: dict[str, int], trial_hours: int) -> list[tuple[str, str]]:
    """Вопросы с подставленными ценами и часами пробного периода."""
    values = {
        "m": prices["1m"], "q": prices["3m"], "y": prices["1y"],
        "pm": round(prices["1y"] / 12), "h": trial_hours,
    }
    return [(q, a.format(**values)) for q, a in _HOME_FAQ]


def home_jsonld(site_url: str, plans: list[dict],
                faq: list[tuple[str, str]]) -> dict:
    """Разметка главной: сайт, приложение с тарифами и FAQ одним @graph,
    чтобы сущности ссылались друг на друга, а не дублировались. faq тот же
    список, что рендерится в видимый блок."""
    offers = [{
        "@type": "Offer",
        "name": p["name"],
        "price": f'{p["price"]}',
        "priceCurrency": "RUB",
        "url": f"{site_url}/pay",
        "availability": "https://schema.org/InStock",
    } for p in plans]
    offers.append({
        "@type": "Offer",
        "name": "Пробный доступ",
        "price": "0",
        "priceCurrency": "RUB",
        "url": f"{site_url}/",
        "availability": "https://schema.org/InStock",
    })
    return {
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": "WebSite",
                "@id": f"{site_url}/#website",
                "url": f"{site_url}/",
                "name": "RadarShield",
                "inLanguage": "ru-RU",
                "publisher": {"@id": f"{site_url}/#org"},
            },
            {
                "@type": "SoftwareApplication",
                "@id": f"{site_url}/#app",
                "name": "RadarShield",
                "url": f"{site_url}/",
                "applicationCategory": "UtilitiesApplication",
                "operatingSystem": "Android, iOS, iPadOS, Windows, macOS",
                "description": (
                    "Сервис умной маршрутизации трафика: подключение остаётся рабочим, "
                    "когда обычные каналы недоступны, а российские сайты и банки идут "
                    "напрямую и не теряют скорость."
                ),
                "inLanguage": "ru-RU",
                "publisher": {"@id": f"{site_url}/#org"},
                "offers": offers,
                "featureList": [
                    "Два транспортных протокола: VLESS Reality и Hysteria2",
                    "Правила маршрутизации: российские сайты идут напрямую",
                    "Одна подписка на Android, iOS, iPadOS, Windows и macOS",
                    "Безлимитный трафик во всех тарифах",
                    "Пробный доступ без регистрации и карты",
                ],
            },
            {
                "@type": "FAQPage",
                "@id": f"{site_url}/#faq",
                "mainEntity": [{
                    "@type": "Question",
                    "name": q,
                    "acceptedAnswer": {"@type": "Answer", "text": a},
                } for q, a in faq],
            },
        ],
    }


def llms_txt(site_url: str, bot_username: str, support_email: str,
             prices: dict[str, int], trial_hours: int) -> str:
    return f"""# RadarShield

> Сервис умной маршрутизации интернет-трафика для России: подключение остаётся
> рабочим, когда обычные каналы недоступны, а российские сайты и банки идут
> напрямую с домашнего адреса и не теряют скорость. Подписка одна на все
> устройства, приложения: FLClash (Android, Windows) и Karing (iOS, iPadOS, macOS).

## Факты

- Платформы: Android, iOS, iPadOS, macOS, Windows.
- Два транспортных протокола, 4 страны, переключение внутри одной подписки.
- Российские ресурсы (Яндекс, VK, Сбер, Ozon, Госуслуги и другие) идут напрямую,
  мимо туннеля, по встроенным в профиль правилам маршрутизации.
- Содержимое трафика не хранится, логи только технические.
- Тарифы: {prices["1m"]} ₽ за месяц, {prices["3m"]} ₽ за 3 месяца,
  {prices["1y"]} ₽ за год ({round(prices["1y"] / 12)} ₽/мес).
- Бесплатно: {trial_hours} часа с сайта без регистрации и карты; 7 дней и 5 ГБ
  через Telegram-бота.
- Оплата: банковская карта, СБП, СберPay, T-Pay (через Robokassa), Telegram Stars.
- Возврат: в первые 24 часа полностью, если подписка не использовалась, позже
  за неиспользованные дни.

## Страницы

- [Главная]({site_url}/): что это, как работает, тарифы, пробный доступ.
- [Инструкция]({site_url}/guide): установка и настройка на Android, iPhone, iPad,
  Windows и macOS, импорт ссылки подписки, частые ошибки.
- [Публичная оферта]({site_url}/offer): условия договора.
- [Политика конфиденциальности]({site_url}/privacy): какие данные собираются.
- [Условия возврата]({site_url}/refund): порядок и сроки возврата денег.
- [Политика безопасности]({site_url}/security): как устроена защита сервиса.

## Контакты

- Telegram-бот: https://t.me/{bot_username}
- Почта поддержки: {support_email}
"""
