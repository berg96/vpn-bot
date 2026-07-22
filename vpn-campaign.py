#!/usr/bin/env python3
"""Раннер кампаний RadarShield.

Всегда двухфазно: `preview` показывает, кому и что уйдёт, `--send` отправляет.
Без `--send` не отправляется НИЧЕГО (требование: Артём читает каждый пуш до прода).

  python3 /root/vpn-campaign.py list
  python3 /root/vpn-campaign.py preview migrate_profile
  python3 /root/vpn-campaign.py send    migrate_profile     # спросит подтверждение
"""
import datetime
import os
import subprocess
import sys
import time
import urllib.parse

sys.path.insert(0, "/root/vpn-bot")
os.environ.setdefault("DB_PATH", "/root/vpn-bot/data/vpn_bot.db")

for _line in open("/root/vpn-bot/.env"):
    _line = _line.strip()
    if "=" in _line and not _line.startswith("#"):
        _k, _v = _line.split("=", 1)
        os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

import campaigns  # noqa: E402
import db  # noqa: E402
import referral  # noqa: E402
import sub_tokens  # noqa: E402

BOT_TOKEN = os.environ["BOT_TOKEN"]
BOT_USERNAME = os.environ.get("BOT_USERNAME", "radarshield_bot")


def _sub_url(tg_id: int) -> str:
    return f"https://radarshield.mooo.com/sub/{sub_tokens.make_sub_token(tg_id)}"


def _install_url(tg_id: int) -> str:
    return f"https://radarshield.mooo.com/open/{sub_tokens.make_sub_token(tg_id)}"


def _age_days(iso_ts: str) -> float:
    """Сколько дней прошло с ISO-метки БД (`first_seen`, tz-aware с микросекундами)."""
    return (
        datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(iso_ts)
    ).total_seconds() / 86400


# ── Кампания: миграция со старого движка ─────────────────────────────────────
# После переезда на Remnawave (08.07) часть клиентов держит профиль, скачанный ДО
# переезда: он бьётся напрямую в старые инбаунды Marzban и работает, пока Marzban
# жив. Погасим Marzban — у них молча отвалится VPN.
#
# ⚠️ Аудитория: ВСЕ с активной подпиской в боевой панели, а не «кто светится в
# Marzban». Прежний критерий врал в обе стороны: `sub_requests` фиксирует только
# факт скачивания подписки, а клиент может держать оба профиля сразу и продолжать
# ходить в Marzban. Единственный надёжный критерий «мигрировал» — ноль коннектов
# в Marzban; проверять его надо ПЕРЕД гашением (`marzban_still_online()`), а не
# при рассылке. Тем, кто уже обновился, сообщение не навредит: текст это учитывает.
#
# Транзакционная: это не реклама, а «иначе у тебя перестанет работать».

_panel_cache: dict[str, dict] | None = None


def _panel_full() -> dict[str, dict]:
    """{username: {"status", "expire", "traffic"}} из БОЕВОЙ панели (Remnawave).

    Кэшируем на процесс: движок зовёт recipients() дважды (preview + send), а
    aiohttp-сессия бэкенда привязана к первому event loop — второй `asyncio.run`
    падает с `Event loop is closed` и выборка молча схлопывается в 0 получателей.
    `expire` — epoch (0 = бессрочно); нужен для окон win_back.
    `traffic` — байты за всё время; 0 = человек ни разу не подключился.
    """
    global _panel_cache
    if _panel_cache is not None:
        return _panel_cache

    import asyncio

    from panel import backend

    users = asyncio.run(backend.list_all_users())
    if not users:
        raise RuntimeError("панель вернула пустой список юзеров — отправку не начинаем")
    _panel_cache = {
        u["username"]: {
            "status": str(u.get("status", "")).lower(),
            "expire": int(u.get("expire") or 0),
            "traffic": int(u.get("lifetime_traffic") or u.get("used_traffic") or 0),
        }
        for u in users
    }
    return _panel_cache


def _panel_users() -> dict[str, str]:
    """{username: status} — обратная совместимость для старых выборок."""
    return {n: v["status"] for n, v in _panel_full().items()}


def _panel_active_usernames() -> set[str]:
    return {n for n, st in _panel_users().items() if st.startswith("activ")}


def marzban_still_online(days: int = 2) -> set[str]:
    """Кто ещё коннектится в старый Marzban. НЕ пусто → гасить панель НЕЛЬЗЯ."""
    sql = f"SELECT username FROM users WHERE online_at >= NOW() - INTERVAL {days} DAY;"
    out = subprocess.run(
        ["docker", "exec", "marzban-db-1", "sh", "-c",
         f'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "USE marzban; {sql}"'],
        capture_output=True, text=True, timeout=30,
    )
    return {ln.strip() for ln in out.stdout.splitlines() if ln.strip()}


def migrate_profile_recipients() -> list[dict]:
    active = _panel_active_usernames()
    return [
        u for u in db.get_reachable_users()
        if u.get("mz_username") in active
    ]


# Инструкция по обновлению профиля — общая для migrate_profile и legacy_profile.
_UPDATE_HOWTO = (
    "<b>Как обновить:</b>\n"
    "• <b>FlClash:</b> вкладка «Профили» → на карточке подписки нажмите "
    "«⋮» (три точки) → «Синхронизация». Или значок ↻ вверху справа — "
    "обновит все профили сразу.\n"
    "• <b>Karing:</b> экран «Профили» → на карточке подписки, рядом с датой "
    "последнего обновления, нажмите значок <b>облачка со стрелкой вниз</b> ☁️.\n\n"
    "<b>Как понять, что всё получилось:</b> в списке серверов станет "
    "<b>12 позиций</b> — по два протокола на каждый сервер (⚡ и 💨)."
)


MIGRATE_PROFILE = campaigns.Campaign(
    name="migrate_profile",
    kind="transactional",
    recipients=migrate_profile_recipients,
    text=(
        "🔄 <b>Обновите профиль в приложении</b>\n\n"
        "Мы обновили движок серверов: их стало больше, и добавился протокол, "
        "который работает там, где обычный VPN уже блокируют.\n\n"
        "Чтобы всё продолжило работать, вам необходимо обновить подписку в "
        "приложении. Если вы уже это сделали — просто проигнорируйте сообщение.\n\n"
        f"{_UPDATE_HOWTO}\n\n"
        "У части пользователей профиль обновится сам — приложение периодически "
        "подтягивает подписку. Поэтому рекомендуем не выключать автоматическое "
        "обновление профиля в настройках: с ним связь остаётся стабильной, а "
        "изменения на серверах доезжают до вас без ручных действий.\n\n"
        "Не нашли кнопку или что-то не заработало — напишите в поддержку, поможем.\n\n"
        "Команда RadarShield"
    ),
    # Кнопки «Обновить профиль» (deep-link на /open/) здесь НЕТ намеренно: она не
    # обновляет существующую подписку, а ДОБАВЛЯЕТ вторую (проверено 14.07). Старый
    # профиль остаётся в клиенте и может остаться выбранным → после гашения Marzban
    # VPN отвалится именно у того, кто «всё сделал по инструкции».
    buttons=lambda u: [
        [{"text": "🆘 Поддержка", "url": "https://t.me/radarshield_support_bot"}],
    ],
    once=True,
)

# ── Кампания: точечная — кто ФАКТИЧЕСКИ ещё ходит через старый Marzban ────────
# Отличие от migrate_profile (тот ушёл всем активным 14.07 «на всякий случай»):
# здесь аудитория считается по РЕАЛЬНОМУ трафику в Marzban, а не по статусу.
# Проверено 18.07: online_at в Marzban шумит — у части юзеров он свежий при нулевом
# трафике (клиент держит старый профиль и переподключается, но человек уже ходит
# через Remnawave). Единственный честный признак — байты в node_user_usages.


def marzban_recent_traffic(days: int = 3, min_mb: int = 100) -> set[str]:
    """Кто РЕАЛЬНО качал через старый Marzban за N дней. Пусто → можно гасить.

    Окно 3 дня, а не 7: на 7 днях в выборку попадал konstantinus_tg со 159 МБ,
    последний раз 14.07 — остаточный хвост второго устройства у человека, который
    сам давно на Remnawave (88 ГБ). Три дня отсекают хвосты и оставляют тех, кто
    ходит через старую панель прямо сейчас.
    """
    sql = (
        "SELECT u.username FROM node_user_usages nu JOIN users u ON u.id=nu.user_id "
        f"WHERE nu.created_at > NOW() - INTERVAL {days} DAY "
        f"GROUP BY u.username HAVING SUM(nu.used_traffic) > {min_mb * 1024 * 1024};"
    )
    out = subprocess.run(
        ["docker", "exec", "marzban-db-1", "sh", "-c",
         f'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "USE marzban; {sql}"'],
        capture_output=True, text=True, timeout=30,
    )
    return {ln.strip() for ln in out.stdout.splitlines() if ln.strip()}


def legacy_profile_recipients() -> list[dict]:
    still = marzban_recent_traffic()
    return [u for u in db.get_reachable_users() if u.get("mz_username") in still]


LEGACY_PROFILE = campaigns.Campaign(
    name="legacy_profile",
    kind="transactional",  # не маркетинг: без обновления человек потеряет доступ
    recipients=legacy_profile_recipients,
    text=(
        "🔄 <b>Ваш профиль нужно обновить</b>\n\n"
        "Недавно мы обновили способ, которым выдаём доступ к серверам. По нашим "
        "данным, на одном из ваших устройств всё ещё используется старый профиль.\n\n"
        "Сейчас он работает, но старые серверы мы скоро отключим — после этого "
        "VPN на этом устройстве перестанет подключаться. Чтобы этого не случилось, "
        "обновите подписку в приложении <b>на всех устройствах</b>, где пользуетесь "
        "RadarShield.\n\n"
        f"{_UPDATE_HOWTO}\n\n"
        "Если возникнут сложности — напишите в нашу службу поддержки, поможем "
        "и всё настроим.\n\n"
        "Команда RadarShield"
    ),
    # Кнопки deep-link нет по той же причине, что и в migrate_profile: она добавляет
    # ВТОРОЙ профиль вместо обновления существующего (проверено 14.07).
    buttons=lambda u: [
        [{"text": "🆘 Написать в поддержку", "url": "https://t.me/radarshield_support_bot"}],
    ],
    # Одно касание на человека за всё время (решение Артёма 18.07): повторять просьбу
    # обновиться — назойливо. Кто не среагировал — к нему идём лично, а не рассылкой.
    # ВНИМАНИЕ: из-за once=True `preview` после отправки всегда показывает 0 и НЕ годится
    # как индикатор готовности гасить Marzban. Для этого — marzban_recent_traffic()
    # напрямую (см. `python3 vpn-campaign.py legacy-check`).
    once=True,
)


# ── Кампании: возврат ушедших (win-back) — два касания по дате истечения ──────
# Аудитория: подписка в панели ЕСТЬ, но НЕ активна, бота не блокировал.
# Триггер — не календарный бласт, а окно по дате истечения:
#   win_back_30 — истекла [30, 90) дней назад (первое касание);
#   win_back_90 — истекла ≥90 дней назад (второе, если на 30 не вернулся).
# Окна не пересекаются → за один прогон юзеру уйдёт максимум одна из двух.
# Каждая `once` → одному человеку каждая уходит один раз за жизнь.
#
# Бонусные дни начисляются ПЕРЕД отправкой (`add_bonus_days`: продлевает от
# сегодня и возвращает статус в ACTIVE), иначе человек нажмёт «попробовать» и
# упрётся в мёртвую подписку — это хуже, чем не писать вовсе.
#
# Требование «есть в панели» обязательно: у части юзеров в нашей БД записан
# mz_username, которого в панели уже нет (удалён при переездах) — им
# `add_bonus_days` упал бы с not found, а сообщение о подарке было бы враньём.
#
# Маркетинговые: под 30-дневный кап и с кнопкой «Реже писать».

WINBACK_BONUS_DAYS = 3           # win-back #1 — 3 дня
WINBACK_SECOND_BONUS_DAYS = 7    # win-back #2 — эскалация: 7 дней (труднее вернуть)
WINBACK_FIRST_AFTER_DAYS = 30    # #1 — через 30 дней после истечения подписки
WINBACK_SECOND_AFTER_DAYS = 63   # #2 — через 63 дня (3 дня триала #1 + 60) ПОСЛЕ #1


def _inactive_in_panel(u: dict, panel: dict) -> dict | None:
    """info из панели, если юзер там есть и НЕ активен; иначе None."""
    info = panel.get(u.get("mz_username"))
    if not info or info["status"].startswith("activ"):
        return None
    return info


def _winback_30_recipients() -> list[dict]:
    """Касание #1: неактивен, прошло ≥30 дней с истечения, win_back_30 в этом
    цикле ещё не слали.

    Якорь — любое истечение (первичный триал или купленная). Повторно с
    3-дневного win-back-триала НЕ сработает: отметка win_back_30 держит `once`
    до сброса. Сброс — только при покупке (`db.delete_winback_notifications`).
    """
    panel = _panel_full()
    now = time.time()
    out = []
    for u in db.get_reachable_users():
        info = _inactive_in_panel(u, panel)
        if not info or not info["expire"]:
            continue
        if (now - info["expire"]) / 86400 < WINBACK_FIRST_AFTER_DAYS:
            continue
        if db.was_notified(u["tg_id"], "win_back_30"):
            continue
        out.append(u)
    return out


def _winback_90_recipients() -> list[dict]:
    """Касание #2: неактивен, win_back_30 ушёл ≥63 дня назад (3 дня триала + 60),
    win_back_90 ещё не слали. Считаем от ДАТЫ ОТПРАВКИ #1, а не от даты в панели
    (её сбивает 3-дневный win-back-триал)."""
    panel = _panel_full()
    out = []
    for u in db.get_reachable_users():
        info = _inactive_in_panel(u, panel)
        if not info:
            continue
        if db.was_notified(u["tg_id"], "win_back_90"):
            continue
        age = db.notification_age_days(u["tg_id"], "win_back_30")
        if age is None or age < WINBACK_SECOND_AFTER_DAYS:
            continue
        out.append(u)
    return out


def grant_bonus_days(users: list[dict], days: int) -> dict:
    """Начислить бонусные дни списку юзеров. → {'ok': n, 'failed': [username, …]}"""
    import asyncio

    async def _run() -> dict:
        # Свежий экземпляр бэкенда: синглтон `panel.backend` мог остаться с
        # aiohttp-сессией от прошлого event loop (`Event loop is closed`).
        from panel import _make_backend

        b = _make_backend()
        ok, failed = 0, []
        for u in users:
            try:
                await b.add_bonus_days(u["mz_username"], days)
                ok += 1
            except Exception as e:  # не роняем всю рассылку из-за одного юзера
                failed.append(f"{u['mz_username']}: {e}")
        return {"ok": ok, "failed": failed}

    return asyncio.run(_run())


def _days_word(n: int) -> str:
    """3 → 'дня', 7 → 'дней' (русское согласование числительного)."""
    if 11 <= n % 100 <= 14:
        return "дней"
    d = n % 10
    if d == 1:
        return "день"
    if 2 <= d <= 4:
        return "дня"
    return "дней"


def _winback_text(days: int) -> str:
    w = _days_word(days)
    return (
        f"🎁 <b>Мы начислили вам {days} {w} — попробуйте снова</b>\n\n"
        "С вашего последнего визита сервис заметно изменился: серверов стало "
        "больше, а главное — добавился протокол, который работает там, где "
        "обычный VPN уже блокируют.\n\n"
        f"Мы продлили вашу подписку на {days} {w}, чтобы вы могли "
        "проверить это без оплаты. Ссылка подписки прежняя — откройте приложение "
        "и включите VPN, заново ничего добавлять не нужно.\n\n"
        "Если приложение не установлено или не сохранилось, или что-то не "
        "работает — напишите в поддержку, поможем подключиться.\n\n"
        "Команда RadarShield"
    )


_WINBACK_BUTTONS = lambda u: [
    [{"text": "🔑 Моя подписка", "url": "https://t.me/radarshield_bot?start=profile"}],
    [{"text": "🆘 Поддержка", "url": "https://t.me/radarshield_support_bot"}],
]

WIN_BACK_30 = campaigns.Campaign(
    name="win_back_30",
    kind="marketing",
    recipients=_winback_30_recipients,
    text=_winback_text(WINBACK_BONUS_DAYS),
    buttons=_WINBACK_BUTTONS,
    once=True,
    meta={"bonus_days": WINBACK_BONUS_DAYS},
)

WIN_BACK_90 = campaigns.Campaign(
    name="win_back_90",
    kind="marketing",
    recipients=_winback_90_recipients,
    text=_winback_text(WINBACK_SECOND_BONUS_DAYS),
    buttons=_WINBACK_BUTTONS,
    once=True,
    meta={"bonus_days": WINBACK_SECOND_BONUS_DAYS},
)

# ── Кампания: служба поддержки + сайт ────────────────────────────────────────
# Информационная: рассказать активной базе про две недоиспользуемые точки входа —
# support-бот и сайт. На фоне РКН-блоков (режут IP нод, бывает режут и домен
# подписки у провайдера) сайт ценен как ЗАПАСНОЙ канал: скачать приложение, взять
# ссылку подписки, продлить/оплатить — всё без бота. Плюс явный адрес поддержки,
# чтобы человек не молчал, когда что-то отвалилось, а писал нам.
#
# ⚠️ Аудитория — РЕШЕНИЕ АРТЁМА (по умолчанию: reachable ∩ панель = реальные
# клиенты, а не всякий, кто нажал /start и не завёл подписку; последних добьёт
# отдельная кампания «ни разу не подключился»). Варианты: только активные /
# все с подпиской / все reachable.
#
# Маркетинговая: под 30-дневный кап и с кнопкой «Реже писать». Кто недавно получил
# win_back (тоже marketing), в эту рассылку не попадёт — движок отсеет сам.


def _site_link(tg_id: int) -> str:
    """Персональная ссылка на главную: даёт лендингу подписанный uid, чтобы браузер
    тихо привязался к аккаунту (link.js) и чат поддержки на сайте узнал человека."""
    return (f"https://radarshield.mooo.com/?uid={tg_id}"
            f"&sig={sub_tokens.make_pay_sig(tg_id)}")


SUPPORT_SITE_FIRST_AFTER_DAYS = 14   # первое касание — D+14 от first_seen
SUPPORT_SITE_INTERVAL_DAYS = 60      # следующие — не чаще раза в 60 дней
SUPPORT_SITE_MAX_SENDS = 3           # всего 3 касания за ~полгода, дальше тишина


def _support_site_recipients() -> list[dict]:
    """Reachable, у кого прошло ≥14 дней с first_seen и не исчерпан лимит касаний.

    D+14, а не раньше: триал 7 дней, и на D+4/D+6/D+7 уже летят напоминания об
    истечении (`bot.py` `_expire_reminders_loop`). Раньше D+14 это сообщение
    конкурировало бы с конверсионным «продлите подписку». Трек повторяющийся —
    первый день некритичен.

    Не сужаем до панели: сообщение полезно и тем, у кого подписки ещё нет —
    текст state-нейтральный, не обещает «продлить» как единственный смысл сайта.
    """
    out = []
    for u in db.get_reachable_users():
        sent = db.notification_count(u["tg_id"], "support_site")
        if sent >= SUPPORT_SITE_MAX_SENDS:
            continue
        if not u.get("first_seen"):
            continue
        if _age_days(u["first_seen"]) < SUPPORT_SITE_FIRST_AFTER_DAYS:
            continue
        if sent:
            last = db.notification_age_days(u["tg_id"], "support_site")
            if last is None or last < SUPPORT_SITE_INTERVAL_DAYS:
                continue
        out.append(u)
    return out


# Три ротируемых варианта: вариант выбирается по счётчику уже отправленных.
# A — базовый (поддержка + сайт + «скажите, что не работает»).
# B — сайт как запасной вход и оплата на нём.
# C — лайфхаки: постоянная ссылка, приложения, куда писать.
# Общий для всех: призыв сообщать, какие приложения/сайты не работают с VPN.
# Это не третья тема, а конкретный повод написать в поддержку вместо
# расплывчатого «если что-то не так», и он же работает на позиционирование
# умной маршрутизации: мы чиним список исключений по обратной связи.

_SS_ROUTING_ASK = (
    "🧭 RadarShield устроен так, чтобы российские сайты, банки и госуслуги шли "
    "мимо VPN — выключать его не нужно.\n"
    "Если что-то всё же не открывается с включённым VPN — напишите нам, какое "
    "приложение или сайт. Разберёмся и поправим у всех."
)


def _support_site_text(user: dict) -> str:
    variant = db.notification_count(user["tg_id"], "support_site")  # 0 → A, 1 → B, 2 → C
    if variant == 0:
        return (
            "📌 <b>Держите под рукой: поддержка и сайт RadarShield</b>\n\n"
            "Две ссылки на случай, если что-то перестанет работать или появятся "
            "вопросы. Прямо сейчас ничего делать не нужно — просто сохраните.\n\n"
            "🆘 <b>Служба поддержки</b> — @radarshield_support_bot\n"
            "Пишите по любому поводу: не подключается VPN, не добавляется профиль, "
            "вопросы по оплате. Поможем настроить и во всём разберёмся.\n\n"
            "🌐 <b>Сайт</b> — radarshield.mooo.com\n"
            "Здесь можно скачать приложение под ваше устройство, оформить или "
            "продлить подписку, а ещё — написать нам в чат прямо на сайте, "
            "если Telegram вдруг окажется недоступен.\n\n"
            f"{_SS_ROUTING_ASK}\n\n"
            "Команда RadarShield"
        )
    if variant == 1:
        return (
            "🌐 <b>Если бот вдруг не откроется — есть сайт</b>\n\n"
            "Бывает, что провайдер режет доступ или Telegram недоступен. На этот "
            "случай всё важное продублировано на сайте — radarshield.mooo.com:\n\n"
            "• скачать приложение под ваше устройство;\n"
            "• оформить или продлить подписку и оплатить картой;\n"
            "• найти свой платёж, если что-то пошло не так.\n\n"
            "Постоянная ссылка на вашу подписку всегда лежит в боте — команда "
            "<code>/profile</code>. Сохраните её отдельно: с ней подписка "
            "восстанавливается в любом приложении за полминуты.\n\n"
            f"{_SS_ROUTING_ASK}\n\n"
            "Команда RadarShield"
        )
    return (
        "💡 <b>Три вещи, которые упрощают жизнь с RadarShield</b>\n\n"
        "1. <b>Ссылка подписки постоянная.</b> Команда <code>/profile</code> в боте — "
        "и она у вас. Меняете телефон или переустанавливаете приложение — просто "
        "вставляете её снова, заводить ничего заново не нужно.\n\n"
        "2. <b>Приложение под любое устройство</b> — на сайте radarshield.mooo.com. "
        "Там же оплата, если бот недоступен.\n\n"
        "3. <b>Поддержка живая</b> — @radarshield_support_bot. Отвечаем и помогаем "
        "настроить, а не отписываемся шаблоном.\n\n"
        f"{_SS_ROUTING_ASK}\n\n"
        "Команда RadarShield"
    )


SUPPORT_SITE = campaigns.Campaign(
    name="support_site",
    kind="marketing",
    recipients=_support_site_recipients,
    text=_support_site_text,
    # Ссылка на сайт ПЕРСОНАЛЬНАЯ (?uid=&sig=): главная отдаёт RS_LINK, link.js тихо
    # привязывает браузер к аккаунту → чат поддержки на сайте сразу знает, кто пишет.
    # Голая главная привязку не запускала — а текст письма её обещает.
    buttons=lambda u: [
        [{"text": "🆘 Написать в поддержку", "url": "https://t.me/radarshield_support_bot"}],
        [{"text": "🌐 Открыть сайт", "url": _site_link(u["tg_id"])}],
    ],
    once=False,  # повторяющийся трек: до 3 касаний, интервал держит _recipients
)

# ── Кампания: застрял на установке (trial_stuck) ─────────────────────────────
# Триал активирован, но конфиг НИ РАЗУ не утянут (ноль sub_requests) → человек
# получил ссылку и не подключился. Самый массовый тихий отвал, и он ещё чинится:
# на D+3 у него остаётся 4 дня триала.
#
# Транзакционное, а не маркетинг: это не реклама, а «у вас не доехало то, за чем
# вы пришли». Под 30-дневный кап не попадает, кнопки «Реже писать» нет.
#
# D+3, а не D+2: даём человеку выходные/пару вечеров самому дойти до установки,
# чтобы не дёргать того, кто просто отложил на завтра. При этом до первого
# напоминания об истечении (D+4) остаётся зазор — сообщения не столкнутся.

TRIAL_STUCK_AFTER_DAYS = 3   # раньше — не дёргаем: человек мог отложить на вечер
TRIAL_STUCK_UNTIL_DAYS = 7   # позже триал (7 дней) уже мёртв, текст стал бы враньём


def _trial_stuck_recipients() -> list[dict]:
    """Триал активирован, идёт 3–7-й день, НОЛЬ ТРАФИКА в панели, ещё не писали.

    ⚠️ Критерий — трафик, а НЕ `sub_requests`. `sub_requests` пишется только на
    роуте `/sub/{token}` (наш HMAC-прокси, `landing/app.py:817`): кто ходит по
    прямой ссылке панели, туда не попадает вообще. Проверено — 3 из 6 платящих
    имеют ноль `sub_requests`, включая самого лояльного клиента (4 покупки). На
    17 исторических кандидатах критерий дал бы 1 ложное срабатывание из 17:
    человеку с 1.92 GB трафика ушло бы «не получилось подключиться?».
    Трафик из панели не зависит от того, каким путём человек забрал конфиг.

    ⚠️ Верхняя граница обязательна. Без неё первый прогон выгреб бы 17 человек с
    `first_seen` от 8 до 97 дней — тех, кто взял триал месяцы назад. Им «пробный
    период ещё идёт» было бы враньём. Кампания проспективная: ловит застрявших
    ПРЯМО СЕЙЧАС, пока триал жив и его ещё можно спасти.
    """
    panel = _panel_full()
    out = []
    for u in db.get_reachable_users():
        if not u.get("trial_activated") or not u.get("first_seen"):
            continue
        if db.was_notified(u["tg_id"], "trial_stuck"):
            continue
        age = _age_days(u["first_seen"])
        if not (TRIAL_STUCK_AFTER_DAYS <= age <= TRIAL_STUCK_UNTIL_DAYS):
            continue
        info = panel.get(u.get("mz_username"))
        if not info or info["traffic"] > 0:  # нет в панели или пользуется — не наш случай
            continue
        out.append(u)
    return out


TRIAL_STUCK = campaigns.Campaign(
    name="trial_stuck",
    kind="transactional",
    recipients=_trial_stuck_recipients,
    text=(
        "🤔 <b>Не получилось подключиться?</b>\n\n"
        "Вы включили пробный период RadarShield, но, судя по всему, VPN так и не "
        "заработал — приложение ни разу не забрало настройки.\n\n"
        "Если застряли на установке — напишите нам. Это обычное дело: подскажем, "
        "что нажать, или настроим вместе, это пара минут. Пробный период ещё идёт, "
        "и будет обидно, если он просто закончится.\n\n"
        "Если передумали — тоже скажите, чем не подошло. Нам это правда полезно.\n\n"
        "Команда RadarShield"
    ),
    buttons=lambda u: [
        [{"text": "📲 Подключить", "url": _install_url(u["tg_id"])}],
        [{"text": "🆘 Помогите настроить", "url": "https://t.me/radarshield_support_bot"}],
    ],
    once=True,
)

# ── Кампания: анонс рефералки (referral_invite) ──────────────────────────────
# Рассказать базе про реф-программу и дать личную ссылку. Окно — D+45 от старта
# (решение Артёма 22.07): человек уже пожил с продуктом, зовёт осознанно. Для всех
# reachable, не только платящих (награда и так срабатывает при оплате друга).
# Маркетинговая, once=True — один раз на юзера. НЕ в AUTO_CAMPAIGNS до утверждения
# текста Артёмом; существующей базе — разовый бласт по команде, дальше в автопилот.

REFERRAL_INVITE_AFTER_DAYS = 45


def _ref_link(tg_id: int) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=ref_{tg_id}"


#: Текст, который подставится другу при нажатии «Поделиться» (от лица юзера).
_REF_SHARE_TEXT = (
    "Пользуюсь RadarShield — умный VPN: российские сайты, банки и госуслуги идут "
    "мимо него, выключать не надо. И работает стабильно, не отваливается. "
    "Переходи по моей ссылке, оформи подписку — нам обоим начислят бонусные дни:"
)


def _ref_share_url(tg_id: int) -> str:
    """URL-кнопка «Поделиться»: открывает шаринг Telegram с личной ссылкой юзера
    и готовым текстом для друга."""
    return ("https://t.me/share/url?url="
            + urllib.parse.quote(_ref_link(tg_id), safe="")
            + "&text=" + urllib.parse.quote(_REF_SHARE_TEXT, safe=""))


def _referral_invite_recipients() -> list[dict]:
    """Reachable, кто прожил ≥45 дней с регистрации. once=True + was_notified в
    eligible() отсекут уже получивших."""
    return [
        u for u in db.get_reachable_users()
        if u.get("first_seen") and _age_days(u["first_seen"]) >= REFERRAL_INVITE_AFTER_DAYS
    ]


def _referral_invite_text(u: dict) -> str:
    d = referral.INVITED_DAYS
    return (
        "🎁 <b>Приглашай друзей — получай бонусные дни</b>\n\n"
        "Отправь другу свою ссылку на RadarShield. Как только он оформит первую "
        f"подписку, <b>вы оба получите +{d} дней</b>. За каждого оплатившего друга — "
        f"ещё +{d} тебе, без ограничений.\n\n"
        "Твоя ссылка:\n"
        f"<code>{_ref_link(u['tg_id'])}</code>\n\n"
        "Команда RadarShield"
    )


REFERRAL_INVITE = campaigns.Campaign(
    name="referral_invite",
    kind="marketing",
    recipients=_referral_invite_recipients,
    text=_referral_invite_text,
    buttons=lambda u: [
        [{"text": "📤 Поделиться ссылкой", "url": _ref_share_url(u["tg_id"])}],
    ],
    once=True,
)

CAMPAIGNS = {c.name: c for c in (
    MIGRATE_PROFILE, LEGACY_PROFILE, WIN_BACK_30, WIN_BACK_90, SUPPORT_SITE, TRIAL_STUCK,
    REFERRAL_INVITE,
)}

# ── Автопилот: ОДНА крон-задача проходит по этому списку ──────────────────────
# Раньше было три крон-строки в 18:00/18:05/18:10. Разнос по времени ничего не
# решал (замер 18.07: каждая кампания отрабатывает <1 сек на 28 юзерах), зато
# порядок кампаний задавался временем в crontab — то есть случайно. А порядок
# ЗНАЧИМ: 30-дневный кап маркетинга достаётся той кампании, что стартовала
# первой. Теперь порядок явный — сверху вниз по списку.
#
# Сюда попадают только кампании, которые можно слать БЕЗ человека. Разовые
# (migrate_profile, legacy_profile) и неутверждённые (trial_stuck) — вручную.
#
# `from_date` — с какой даты кампания участвует в автопилоте (None = сразу).
# support_site: 21.07 её шлёт one-shot бласт в обход капа, автопилот подхватывает
# со следующего дня — так первое касание уходит всем сразу, а дальше каждый новый
# пользователь получает письмо на своём D+14.
AUTO_CAMPAIGNS = [
    # (имя, from_date) — транзакционные выше маркетинговых: без них у человека
    # что-то ломается, а маркетинг подождёт до следующего окна.
    ("trial_stuck", None),
    ("win_back_30", None),
    ("win_back_90", None),
    ("support_site", datetime.date(2026, 7, 22)),
    # referral_invite (D+45 от старта) — с 26.07: 25.07 старую базу накрывает разовый
    # nocap-бласт (в обход капа), а с воскресенья автопилот ловит НОВЫХ на их D+45.
    # Ниже support_site по приоритету капа (реф-приглашение менее срочно).
    ("referral_invite", datetime.date(2026, 7, 26)),
]


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 1
    cmd = sys.argv[1]

    if cmd == "list":
        for name, c in CAMPAIGNS.items():
            print(f"{name:20} {c.kind:14} получателей сейчас: {len(campaigns.eligible(c))}")
        return 0

    if cmd == "legacy-check":
        # Готов ли Marzban к гашению. Отдельная команда, а не preview legacy_profile:
        # та кампания once=True и после отправки всегда показывает 0 получателей.
        still = marzban_recent_traffic()
        if not still:
            print("✅ Живого трафика в Marzban за 3 дня нет — панель можно гасить.")
            return 0
        print(f"🔴 Ещё ходят через Marzban ({len(still)}): {', '.join(sorted(still))}")
        print("   Гасить НЕЛЬЗЯ — эти люди останутся без VPN.")
        return 1

    if cmd != "autosend-all" and (len(sys.argv) < 3 or sys.argv[2] not in CAMPAIGNS):
        print(f"Укажи кампанию: {', '.join(CAMPAIGNS)}")
        return 1
    c = CAMPAIGNS[sys.argv[2]] if cmd != "autosend-all" else None

    if cmd == "preview":
        print(campaigns.preview(c).replace("<b>", "").replace("</b>", "")
              .replace("<code>", "").replace("</code>", ""))
        return 0

    if cmd == "send":
        users = campaigns.eligible(c)
        if not users:
            print("Получателей 0 — отправлять нечего. Отменено.")
            return 1
        print(campaigns.preview(c))
        bonus = c.meta.get("bonus_days")
        if bonus:
            print(f"\n⚠️ Перед отправкой всем {len(users)} будет начислено "
                  f"{bonus} дня подписки (статус → ACTIVE).")
        print(f"\nОТПРАВИТЬ {len(users)} сообщений? Введи 'yes': ", end="")
        if input().strip().lower() != "yes":
            print("Отменено.")
            return 1

        if bonus:
            # Сначала дни, потом текст: иначе человек придёт на мёртвую подписку.
            res = grant_bonus_days(users, bonus)
            print(f"Начислено дней: {res['ok']}/{len(users)}")
            for f in res["failed"]:
                print(f"  ⚠️ не начислено — {f}")
            if not res["ok"]:
                print("Ни одного начисления не прошло — рассылку не начинаем.")
                return 1

        stats = campaigns.send(BOT_TOKEN, c, dry_run=False)
        print("Результат:", stats)
        return 0

    if cmd == "autosend-all":
        # Единая крон-задача: один проход по AUTO_CAMPAIGNS в явном порядке.
        # Падение одной кампании не должно ронять остальные — отсюда try/except.
        today = datetime.date.today()
        total = {"sent": 0, "blocked": 0, "failed": 0}
        for name, from_date in AUTO_CAMPAIGNS:
            camp = CAMPAIGNS[name]
            if from_date and today < from_date:
                print(f"{name}: автопилот с {from_date} — пропуск.")
                continue
            try:
                users = campaigns.eligible(camp)
                if not users:
                    print(f"{name}: получателей 0 — пропуск.")
                    continue
                bonus = camp.meta.get("bonus_days")
                if bonus:
                    res = grant_bonus_days(users, bonus)
                    print(f"{name}: начислено дней {res['ok']}/{len(users)}")
                    if not res["ok"]:
                        print(f"{name}: ни одного начисления — рассылку не начинаем.")
                        continue
                stats = campaigns.send(BOT_TOKEN, camp, dry_run=False)
                print(f"{name}: {stats}")
                for k in total:
                    total[k] += stats.get(k, 0)
            except Exception as e:
                print(f"{name}: ОШИБКА — {e!r}")
        print(f"ИТОГО: {total}")
        return 0

    if cmd == "autosend-nocap":
        # Разовый бласт В ОБХОД 30-дневного капа маркетинга. Только по явному
        # решению Артёма (18.07: «тем, кому win_back пришёл, тоже отправить»).
        # Ставится в крон ОДНОЙ датой и снимается после — не как ежедневная задача.
        users = campaigns.eligible(c, ignore_cap=True)
        if not users:
            print(f"{c.name}: получателей 0 — пропуск.")
            return 0
        stats = campaigns.send(BOT_TOKEN, c, dry_run=False, ignore_cap=True)
        print(f"{c.name} (nocap): {stats}")
        return 0

    if cmd == "preview-nocap":
        users = campaigns.eligible(c, ignore_cap=True)
        print(f"Кампания: {c.name} ({c.kind}) — БЕЗ капа")
        print(f"Получателей: {len(users)}")
        print("Кому:", ", ".join(str(u["tg_id"]) for u in users))
        return 0

    if cmd == "autosend":
        # Не-интерактивная отправка для КРОНА (без гейта «yes»). Идемпотентность
        # и окна держит сам движок (once + eligible), так что повторный вызов
        # ничего не задваивает. ⚠️ Ставить в crontab только после явной отмашки:
        # это автоматическая рассылка живым людям.
        users = campaigns.eligible(c)
        if not users:
            print(f"{c.name}: получателей 0 — пропуск.")
            return 0
        bonus = c.meta.get("bonus_days")
        if bonus:
            res = grant_bonus_days(users, bonus)
            print(f"{c.name}: начислено дней {res['ok']}/{len(users)}")
            for f in res["failed"]:
                print(f"  ⚠️ не начислено — {f}")
            if not res["ok"]:
                print(f"{c.name}: ни одного начисления не прошло — рассылку не начинаем.")
                return 1
        stats = campaigns.send(BOT_TOKEN, c, dry_run=False)
        print(f"{c.name}: {stats}")
        return 0

    print(f"Неизвестная команда: {cmd}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
