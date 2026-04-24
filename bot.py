import asyncio
import logging
import os
from typing import Any, Awaitable, Callable
import aiohttp
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.types import (
    Message, CallbackQuery, Update, TelegramObject,
    InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery,
)
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.enums import ParseMode

import config
import db
import marzban
import tinkoff


def _resolve_mz_username(tg_id: int, tg_username: str | None, first_name: str | None) -> str:
    """Возвращает mz_username из DB или генерирует новый для нового пользователя."""
    stored = db.get_mz_username(tg_id)
    if stored:
        return stored
    # Для существующих без записи — оставляем старый формат (backward compat)
    return f"tg_{tg_id}"


def _assign_mz_username(tg_id: int, tg_username: str | None, first_name: str | None) -> str:
    """Генерирует и сохраняет красивое имя для нового пользователя."""
    stored = db.get_mz_username(tg_id)
    if stored:
        return stored
    new_name = marzban.build_mz_username(tg_id, tg_username, first_name)
    db.set_mz_username(tg_id, new_name)
    return new_name


LANDING_BASE_URL = "https://radarshield.mooo.com"

import sub_tokens  # noqa: E402


def _stable_sub_url(tg_id: int) -> str:
    """Стабильная HMAC-ссылка подписки — никогда не меняется для tg_id.

    Прокси на лендинге расшифровывает токен → ищет mz_username по tg_id →
    отдаёт актуальный sub из Marzban. Продление/смена лимита/ротация
    конфигов — клиент не видит, ссылка та же.
    """
    return f"{LANDING_BASE_URL}/sub/{sub_tokens.make_sub_token(tg_id)}"


def _install_url(mz_name: str) -> str:
    """URL для кнопки 'Открыть в приложении' в боте — тот же deep-link flow что на сайте."""
    return f"{LANDING_BASE_URL}/open/{mz_name}"


def _bonus_applied_text(expire_str: str, sub_url: str) -> str:
    """Единое сообщение после применения +1 дня бонуса (merge или реактивация)."""
    return (
        f"🎁 <b>+1 день к твоей подписке.</b>\n\n"
        f"📅 Активна до <b>{expire_str}</b>.\n\n"
        f"🔗 Твоя ссылка:\n<code>{sub_url}</code>\n\n"
        "⚠️ <b>На устройстве, с которого ты пришёл с сайта</b>, замени ссылку на эту — "
        "та, что ты получил с лендинга, истечёт через 3 часа и перестанет работать. "
        "На остальных устройствах ничего менять не нужно.\n\n"
        "Нажми <b>📲 Открыть в приложении</b> — клиент откроется с готовым импортом."
    )


def _bonus_applied_keyboard(current_mz: str) -> "InlineKeyboardMarkup":
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="📲 Открыть в приложении", url=_install_url(current_mz))],
        [InlineKeyboardButton(text="📖 Установка", callback_data="apps_menu")],
        [InlineKeyboardButton(text="💬 Поддержка", url=config.SUPPORT_LINK)],
    ])


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()


class ActivityMiddleware(BaseMiddleware):
    """Bumps users.last_seen on every interaction. Silent — never blocks handlers."""

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        try:
            user = data.get("event_from_user")
            if user and db.user_exists(user.id):
                db.touch_user(user.id)
        except Exception as e:
            logger.debug(f"touch_user failed: {e}")
        return await handler(event, data)


dp.message.middleware(ActivityMiddleware())
dp.callback_query.middleware(ActivityMiddleware())


# ── Клавиатуры ──────────────────────────────────────────────────────────────

def _pay_url(tg_id: int) -> str:
    """Подписанная ссылка на страницу оплаты — защищает от подстановки чужого tg_id."""
    import hmac, hashlib
    secret = os.environ.get("PAY_LINK_SECRET", config.BOT_TOKEN[:32])
    sig = hmac.new(secret.encode(), str(tg_id).encode(), hashlib.sha256).hexdigest()[:16]
    return f"{LANDING_BASE_URL}/pay?uid={tg_id}&sig={sig}"


def plans_keyboard(tg_id: int | None = None) -> InlineKeyboardMarkup:
    buttons = []
    for key, (name, days, stars, stars_str, rub_kopeks, rub_str) in config.PLANS.items():
        buttons.append([InlineKeyboardButton(
            text=f"{name} — {stars_str}",
            callback_data=f"buy:{key}",  # Telegram Stars fallback
        )])
    if tg_id:
        buttons.append([InlineKeyboardButton(
            text="💳 Оплатить картой на сайте", url=_pay_url(tg_id),
        )])
    buttons.append([
        InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
        InlineKeyboardButton(text="📖 Установка", callback_data="apps_menu"),
    ])
    buttons.append([InlineKeyboardButton(text="💬 Поддержка", url=config.SUPPORT_LINK)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Установка", callback_data="apps_menu")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        [InlineKeyboardButton(text="💬 Поддержка", url=config.SUPPORT_LINK)],
    ])


def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="← Назад", callback_data="back_to_plans")]
    ])


FLCLASH_ANDROID = "https://github.com/chen08209/FlClash/releases/download/v0.8.92/FlClash-0.8.92-android-arm64-v8a.apk"
FLCLASH_WINDOWS = "https://github.com/chen08209/FlClash/releases/download/v0.8.92/FlClash-0.8.92-windows-amd64-setup.exe"
FLCLASH_MACOS_ARM = "https://github.com/chen08209/FlClash/releases/download/v0.8.92/FlClash-0.8.92-macos-arm64.dmg"
FLCLASH_MACOS_X64 = "https://github.com/chen08209/FlClash/releases/download/v0.8.92/FlClash-0.8.92-macos-amd64.dmg"

APPS_TEXT = {
    "android": (
        "📱 <b>Android — FLClash</b>\n\n"
        "1. Скачай FLClash (кнопка ниже) и установи APK\n\n"
        "2. Открой бота → /profile → скопируй ссылку\n\n"
        "3. В FLClash: нажми <b>+</b> → <b>Import from URL</b>\n\n"
        "4. Вставь ссылку → нажми <b>Подключить</b>\n\n"
        "✅ Готово!"
    ),
    "ios": (
        "📱 <b>iOS — Karing</b>\n\n"
        "1. Установи <a href='https://apps.apple.com/app/karing/id6472431552'>Karing</a> из App Store\n\n"
        "2. Открой бота → /profile → скопируй ссылку подписки\n\n"
        "3. В Karing: нажми <b>+</b> → <b>Импорт из буфера обмена</b>\n\n"
        "4. Нажми <b>Connect</b>\n\n"
        "✅ Готово! При первом запуске разреши добавление VPN-профиля."
    ),
    "windows": (
        "💻 <b>Windows — FLClash</b>\n\n"
        "1. Скачай FLClash (кнопка ниже) и установи\n\n"
        "2. Открой бота → /profile → скопируй ссылку\n\n"
        "3. В FLClash: нажми <b>+</b> → <b>Import from URL</b>\n\n"
        "4. Вставь ссылку → нажми <b>Подключить</b>\n\n"
        "✅ Готово!"
    ),
    "macos": (
        "💻 <b>macOS — FLClash</b>\n\n"
        "1. Скачай FLClash (кнопка ниже — выбери свой Mac)\n\n"
        "2. Открой бота → /profile → скопируй ссылку\n\n"
        "3. В FLClash: нажми <b>+</b> → <b>Import from URL</b>\n\n"
        "4. Вставь ссылку → нажми <b>Подключить</b>\n\n"
        "✅ Готово!"
    ),
    "routing": (
        "🔧 <b>Умная маршрутизация</b>\n\n"
        "Российские сайты (VK, Яндекс, Госуслуги, Сбер и др.) открываются напрямую — "
        "без VPN и без потери скорости.\n"
        "Всё остальное идёт через VPN.\n\n"
        "Маршрутизация встроена в профиль автоматически.\n"
        "Просто добавь ссылку подписки в FLClash — всё настроится само."
    ),
}

def apps_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🤖 Android", callback_data="apps:android"),
            InlineKeyboardButton(text="🍎 iOS", callback_data="apps:ios"),
        ],
        [
            InlineKeyboardButton(text="🪟 Windows", callback_data="apps:windows"),
            InlineKeyboardButton(text="🍏 macOS", callback_data="apps:macos"),
        ],
        [InlineKeyboardButton(text="🔧 Умная маршрутизация", callback_data="apps:routing")],
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
    ])


# ── /start ───────────────────────────────────────────────────────────────────

async def _do_start(tg_id: int, username: str | None, first_name: str | None, answer_fn) -> None:
    """Общая логика старта — используется и в /start и в кнопке 'Начать'."""
    is_new = db.record_user(tg_id, username)
    if is_new:
        db.log_event(tg_id, "user_registered", {"username": username})
    db.touch_user(tg_id)
    db.log_event(tg_id, "start")

    if not db.is_trial_used(tg_id):
        await answer_fn(
            "🔐 <b>RadarShield VPN</b>\n\n"
            "⏳ Активирую пробный период...",
            parse_mode=ParseMode.HTML,
        )
        try:
            async with aiohttp.ClientSession() as session:
                mz_name = _assign_mz_username(tg_id, username, first_name)
                user_data = await marzban.create_trial_user(session, tg_id, days=7, data_limit_gb=5.0, mz_username=mz_name)
            db.mark_trial_used(tg_id)
            db.log_event(tg_id, "trial_activated", {"days": 7, "data_limit_gb": 5.0})
            mz_sub_url = user_data.get("subscription_url", "")
            if mz_sub_url:
                db.set_sub_url(tg_id, mz_sub_url)
            sub_url = _stable_sub_url(tg_id)
            await answer_fn(
                "🎁 <b>Тебе активирован бесплатный период на 7 дней (5 ГБ)</b>\n\n"
                "🔗 Ссылка для подключения:\n"
                f"<code>{sub_url}</code>\n\n"
                "📱 <b>Как подключиться:</b>\n"
                "• <b>Android / Windows / macOS</b> → FLClash\n"
                "• <b>iOS</b> → Karing\n\n"
                "Нажми <b>📲 Открыть в приложении</b> — импортируется сразу 👇",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
                    [InlineKeyboardButton(text="📲 Открыть в приложении", url=_install_url(mz_name))],
                    [InlineKeyboardButton(text="📖 Установка", callback_data="apps_menu")],
                    [InlineKeyboardButton(text="💬 Поддержка", url=config.SUPPORT_LINK)],
                ]),
            )
        except Exception as e:
            logger.error(f"Trial creation failed for {tg_id}: {e}")
            await answer_fn(
                "🔐 <b>VPN — быстро, надёжно, без лишних вопросов</b>\n\n"
                "Выбери тариф:",
                parse_mode=ParseMode.HTML,
                reply_markup=start_keyboard(),
            )
    else:
        await answer_fn(
            "🔐 <b>RadarShield VPN</b>\n\n"
            "Работает на Android, iOS, Windows, macOS.\n\n",
            parse_mode=ParseMode.HTML,
            reply_markup=start_keyboard(),
        )


@dp.message(CommandStart())
async def cmd_start(msg: Message, command: CommandObject):
    """Обрабатывает /start c payload и без.

    Поддерживаемые payload'ы:
      lp_<token> → landing-конверсия (+7 дней нового / +1 день существующему)
      profile    → сразу открыть /profile (deep-link из webhook'а RUB-оплаты)
      (пусто)    → обычное приветствие + активация триала, если первый раз
    """
    if command.args:
        if command.args.startswith("lp_"):
            await _handle_landing_deeplink(msg, command.args[3:])
            return
        if command.args == "profile":
            await cmd_profile(msg)
            return
    await _do_start(msg.from_user.id, msg.from_user.username, msg.from_user.first_name, msg.answer)


async def _handle_landing_deeplink(msg: Message, token: str) -> None:
    tg_id = msg.from_user.id
    username = msg.from_user.username
    first_name = msg.from_user.first_name

    lead = db.get_landing_lead(token)
    if not lead or lead["claimed_tg_id"] is not None:
        await _do_start(tg_id, username, first_name, msg.answer)
        return

    is_new = not db.user_exists(tg_id)
    has_active_sub = False
    current_mz = None
    if not is_new:
        current_mz = db.get_mz_username(tg_id)
        if current_mz:
            try:
                async with aiohttp.ClientSession() as session:
                    existing = await marzban.get_user(session, tg_id, current_mz)
                if existing and existing.get("status") == "active":
                    # Marzban сам считает status. active = действует (включая expire=0 — unlimited).
                    # Ранее была проверка (expire or 0) > now_ts, и unlimited (expire=0)
                    # ошибочно считался просроченным — перезаписывался landing-профилем.
                    has_active_sub = True
            except Exception as e:
                logger.error(f"get_user failed: {e}")

    if has_active_sub:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="🎁 +1 день к подписке",
                callback_data=f"lp_merge:{token}",
            )],
        ])
        await msg.answer(
            "👋 <b>У тебя уже есть подписка в RadarShield.</b>\n\n"
            "Ты зашёл с сайта — можешь забрать <b>+1 день в подарок</b> к своей подписке.\n\n"
            "На устройстве, с которого пришёл, используй свою основную ссылку — "
            "её можно взять в /profile. Она же работает на всех остальных твоих устройствах.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return

    # Существующий юзер без активной подписки → реактивация на +1 день (не 7).
    # Только один раз за всё время; повторно попытка → отказ.
    if not is_new:
        if db.count_events(tg_id, "lp_merge") > 0:
            # Бонус уже брал раньше — закрываем lead, не даём новый.
            # Marzban-юзер НЕ удаляем: пусть 3ч протухает сам, юзер успеет использовать.
            if lead["claimed_tg_id"] is None:
                db.claim_landing_lead(token, tg_id)
            db.log_event(tg_id, "lp_merge_declined", {"token": token, "reason": "already_received"})
            await msg.answer(
                "⚠️ <b>Бонус уже получен ранее.</b>\n\n"
                "Повторно применить нельзя. Для продления подписки выбери тариф:",
                parse_mode=ParseMode.HTML,
                reply_markup=plans_keyboard(tg_id=tg_id),
            )
            return

        # Реактивируем старый mz_user на +1 день
        claimed = db.claim_landing_lead(token, tg_id)
        if not claimed:
            await _do_start(tg_id, username, first_name, msg.answer)
            return
        db.touch_user(tg_id)
        db.log_event(tg_id, "landing_claim", {"token": token})

        try:
            async with aiohttp.ClientSession() as session:
                # Landing-профиль не удаляем — 3ч сам протухнет, даёт юзеру время
                # переключиться на основную ссылку.
                updated = await marzban.extend_user(
                    session,
                    mz_username=current_mz,
                    total_days=1,
                    data_limit_gb=5.0,
                )
                # Синхронизируем sub_url landing-лида со свежим от Marzban:
                # если cleanup делал revoke_sub, UUID поменялся — на сайте покажем актуальный.
                lp_user = await marzban.get_user(session, 0, mz_username=lead["mz_username"])
                if lp_user and lp_user.get("subscription_url"):
                    db.set_landing_sub_url(token, lp_user["subscription_url"])
            mz_sub_url = updated.get("subscription_url") or ""
            if mz_sub_url:
                db.set_sub_url(tg_id, mz_sub_url)
            sub_url = _stable_sub_url(tg_id)
            db.log_event(tg_id, "lp_merge", {"token": token, "bonus_days": 1, "reactivation": True})

            expire_ts = updated.get("expire")
            expire_str = (
                datetime.fromtimestamp(expire_ts, tz=timezone.utc).strftime("%d.%m.%Y")
                if expire_ts else "—"
            )
            await msg.answer(
                _bonus_applied_text(expire_str, sub_url),
                parse_mode=ParseMode.HTML,
                reply_markup=_bonus_applied_keyboard(current_mz),
            )

        except Exception as e:
            logger.error(f"reactivation failed for {token}: {e}")
            await msg.answer(
                "❌ Что-то пошло не так при активации. Напиши в поддержку: " + config.SUPPORT_LINK,
            )
        return

    # Новый юзер → auto-merge в 7д/5ГБ (маркетинг лендинга).
    claimed = db.claim_landing_lead(token, tg_id)
    if not claimed:
        await _do_start(tg_id, username, first_name, msg.answer)
        return

    db.record_user(tg_id, username)
    db.log_event(tg_id, "user_registered", {"username": username, "source": "landing"})
    db.touch_user(tg_id)
    db.log_event(tg_id, "landing_claim", {"token": token})

    await msg.answer("⏳ Активирую 7 дней и 5 ГБ...")

    try:
        async with aiohttp.ClientSession() as session:
            user_data = await marzban.extend_user(
                session,
                mz_username=lead["mz_username"],
                total_days=7,
                data_limit_gb=5.0,
            )
        db.set_mz_username(tg_id, lead["mz_username"])
        db.mark_trial_used(tg_id)
        db.delete_reminder_events(tg_id)  # свежая подписка → reset напоминаний
        db.log_event(tg_id, "trial_activated", {"days": 7, "data_limit_gb": 5.0, "source": "landing"})

        mz_sub_url = user_data.get("subscription_url", "")
        if mz_sub_url:
            db.set_sub_url(tg_id, mz_sub_url)
        sub_url = _stable_sub_url(tg_id)

        await msg.answer(
            "🎁 <b>Готово! 7 дней и 5 ГБ активированы</b>\n\n"
            f"🔗 Твоя ссылка:\n<code>{sub_url}</code>\n\n"
            "Нажми <b>📲 Открыть в приложении</b> — клиент откроется с готовым импортом.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
                [InlineKeyboardButton(text="📲 Открыть в приложении", url=_install_url(lead["mz_username"]))],
                [InlineKeyboardButton(text="📖 Установка", callback_data="apps_menu")],
                [InlineKeyboardButton(text="💬 Поддержка", url=config.SUPPORT_LINK)],
            ]),
        )

    except Exception as e:
        logger.error(f"landing extend_user failed for {token}: {e}")
        await msg.answer(
            "❌ Что-то пошло не так при активации. Напиши в поддержку: " + config.SUPPORT_LINK,
        )


@dp.callback_query(F.data.startswith("lp_merge:"))
async def cb_lp_merge(call: CallbackQuery):
    token = call.data.split(":", 1)[1]
    tg_id = call.from_user.id

    lead = db.get_landing_lead(token)
    if not lead:
        await call.answer("Пробный период не найден", show_alert=True)
        return
    if lead["claimed_tg_id"] is not None and lead["claimed_tg_id"] != tg_id:
        await call.answer("Пробный период уже использован другим аккаунтом", show_alert=True)
        return

    # Анти-абуз: +1 день можно забрать только один раз за всё время.
    # Иначе юзер мог бы плодить lead'ы с разных браузеров и бесконечно продлять подписку.
    if db.count_events(tg_id, "lp_merge") > 0:
        # Закрываем lead — на сайте "7 дней в TG" уже не покажется.
        # Marzban-юзера НЕ удаляем: пробный период 3ч остаётся рабочим на устройстве,
        # откуда юзер зашёл на сайт — пусть пользуется.
        if lead["claimed_tg_id"] is None:
            db.claim_landing_lead(token, tg_id)
        db.log_event(tg_id, "lp_merge_declined", {"token": token, "reason": "already_received"})

        await call.message.edit_text(
            "⚠️ <b>Бонус +1 день уже получен ранее.</b>\n\n"
            "Актуальная ссылка на твою подписку — в /profile. Используй её на устройстве, "
            "с которого ты пришёл с сайта.\n\n"
            "Для продления подписки — выбери тариф ниже.",
            parse_mode=ParseMode.HTML,
            reply_markup=plans_keyboard(tg_id=call.from_user.id),
        )
        await call.answer()
        return

    current_mz = db.get_mz_username(tg_id)
    if not current_mz:
        await call.answer("Не нашёл твою текущую подписку", show_alert=True)
        return

    if lead["claimed_tg_id"] is None:
        if not db.claim_landing_lead(token, tg_id):
            await call.answer("Не удалось закрепить пробный период", show_alert=True)
            return

    try:
        async with aiohttp.ClientSession() as session:
            # Не удаляем landing-профиль сразу: у него свой expire +3h, он сам протухнет,
            # а cleanup-loop потом дисейблит. Даём юзеру время переключиться на основную
            # ссылку без резкого обрыва на том устройстве, с которого он пришёл с сайта.
            updated = await marzban.add_bonus_days(session, current_mz, bonus_days=1)
            # Синхронизируем sub_url landing-лида на случай если он был revoked ранее —
            # чтобы на сайте показывалась актуальная ссылка.
            lp_user = await marzban.get_user(session, 0, mz_username=lead["mz_username"])
            if lp_user and lp_user.get("subscription_url"):
                db.set_landing_sub_url(token, lp_user["subscription_url"])
    except Exception as e:
        logger.error(f"lp_merge failed: {e}")
        await call.answer("Ошибка, напиши в поддержку", show_alert=True)
        return

    mz_sub_url = updated.get("subscription_url") or db.get_sub_url(tg_id) or ""
    if mz_sub_url:
        db.set_sub_url(tg_id, mz_sub_url)
    sub_url = _stable_sub_url(tg_id)
    expire_ts = updated.get("expire")
    expire_str = (
        datetime.fromtimestamp(expire_ts, tz=timezone.utc).strftime("%d.%m.%Y")
        if expire_ts else "—"
    )
    db.log_event(tg_id, "lp_merge", {"token": token, "bonus_days": 1})

    await call.message.edit_text(
        _bonus_applied_text(expire_str, sub_url),
        parse_mode=ParseMode.HTML,
        reply_markup=_bonus_applied_keyboard(current_mz),
    )
    await call.answer("+1 день 🎁")




# ── /profile ─────────────────────────────────────────────────────────────────

@dp.message(Command("profile"))
@dp.callback_query(F.data == "profile")
async def cmd_profile(event: Message | CallbackQuery):
    msg = event.message if isinstance(event, CallbackQuery) else event
    tg_id = event.from_user.id

    if not db.user_exists(tg_id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Начать", callback_data="do_start")]
        ])
        if isinstance(event, CallbackQuery):
            await event.message.edit_text(
                "👋 Ты ещё не зарегистрирован. Нажми кнопку — активирую бесплатный период.",
                reply_markup=kb,
            )
            await event.answer()
        else:
            await msg.answer(
                "👋 Ты ещё не зарегистрирован. Нажми кнопку — активирую бесплатный период.",
                reply_markup=kb,
            )
        return

    mz_name = _resolve_mz_username(tg_id, event.from_user.username, event.from_user.first_name)
    async with aiohttp.ClientSession() as session:
        user = await marzban.get_user(session, tg_id, mz_name)

    if not user:
        if not db.is_trial_used(tg_id):
            # Триал не использован — предложить начать
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🚀 Начать", callback_data="do_start")]
            ])
            text = "👋 Нажми кнопку — активирую бесплатный период на 7 дней и 5 ГБ."
        else:
            text = "У тебя пока нет активной подписки.\n\nВыбери тариф ниже 👇"
            kb = plans_keyboard(tg_id=tg_id)
    else:
        status = user.get("status", "unknown")
        expire_ts = user.get("expire")
        if expire_ts:
            expire_dt = datetime.fromtimestamp(expire_ts, tz=timezone.utc)
            expire_str = expire_dt.strftime("%d.%m.%Y")
            # ceil: "7д 23ч" показываем как "8 дней", иначе +1 бонус "теряется" при выводе
            seconds_left = (expire_dt - datetime.now(timezone.utc)).total_seconds()
            days_left = max(0, -(-int(seconds_left) // 86400))
            expire_line = f"📅 Истекает: <b>{expire_str}</b> (через {days_left} дн.)"
        else:
            expire_line = "📅 Срок: <b>бессрочно</b>"

        mz_sub_url = user.get("subscription_url") or ""
        if mz_sub_url and not db.get_sub_url(tg_id):
            db.set_sub_url(tg_id, mz_sub_url)
        sub_url = _stable_sub_url(tg_id)
        status_emoji = {"active": "✅", "expired": "❌", "disabled": "🚫"}.get(status, "❓")

        text = (
            f"{status_emoji} Статус: <b>{status}</b>\n"
            f"{expire_line}\n\n"
            f"🔗 Ссылка для подключения:\n<code>{sub_url}</code>\n\n"
            "Импортируй ссылку в приложение: FLClash (Android/Windows/macOS), "
            "Karing (iOS)."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Продлить", callback_data="back_to_plans")],
            [InlineKeyboardButton(text="📲 Открыть в приложении", url=_install_url(mz_name))],
            [InlineKeyboardButton(text="📖 Установка", callback_data="apps_menu")],
            [InlineKeyboardButton(text="💬 Поддержка", url=config.SUPPORT_LINK)],
        ])

    if isinstance(event, CallbackQuery):
        try:
            await event.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass
        await event.answer()
    else:
        await msg.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)


# ── Выбор тарифа ─────────────────────────────────────────────────────────────

@dp.callback_query(F.data.startswith("buy:"))
async def cb_buy(call: CallbackQuery):
    plan_key = call.data.split(":")[1]
    plan = config.PLANS.get(plan_key)
    if not plan:
        await call.answer("Неизвестный тариф", show_alert=True)
        return

    name, days, stars, stars_str, rub_kopeks, rub_str = plan
    db.log_event(call.from_user.id, "pay_initiated", {"plan": plan_key, "stars": stars})
    await call.answer()
    await bot.send_invoice(
        chat_id=call.from_user.id,
        title=f"VPN — {name}",
        description=f"VLESS Reality, без ограничений трафика, {days} дней",
        payload=f"vpn:{plan_key}:{call.from_user.id}",
        currency="XTR",
        prices=[LabeledPrice(label=f"VPN {name}", amount=stars)],
    )


@dp.callback_query(F.data == "back_to_plans")
async def cb_back(call: CallbackQuery):
    await call.message.edit_text(
        "Выбери тариф:",
        reply_markup=plans_keyboard(tg_id=call.from_user.id),
    )
    await call.answer()


# ── Установка приложений ──────────────────────────────────────────────────────

@dp.message(Command("apps"))
@dp.callback_query(F.data == "apps_menu")
async def cmd_apps_menu(event: Message | CallbackQuery):
    tg_id = event.from_user.id
    if not db.user_exists(tg_id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Начать", callback_data="do_start")]
        ])
        if isinstance(event, CallbackQuery):
            await event.message.edit_text(
                "👋 Нажми кнопку — активирую бесплатный период на 7 дней и 5 ГБ.",
                reply_markup=kb,
            )
            await event.answer()
        else:
            await event.answer(
                "👋 Нажми кнопку — активирую бесплатный период на 7 дней и 5 ГБ.",
                reply_markup=kb,
            )
        return

    text = (
        "📲 <b>Как подключить VPN</b>\n\n"
        "Выбери свою платформу — дам пошаговую инструкцию:"
    )
    kb = apps_keyboard()
    if isinstance(event, CallbackQuery):
        await event.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        await event.answer()
    else:
        await event.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@dp.callback_query(F.data.startswith("apps:"))
async def cb_apps_platform(call: CallbackQuery):
    platform = call.data.split(":")[1]
    text = APPS_TEXT.get(platform)
    if not text:
        await call.answer("Неизвестная платформа", show_alert=True)
        return

    if platform == "routing":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Выбрать платформу", callback_data="apps_menu")],
        ])
    elif platform == "android":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬇️ Скачать FLClash (Android)", url=FLCLASH_ANDROID)],
            [InlineKeyboardButton(text="← Выбрать платформу", callback_data="apps_menu")],
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        ])
    elif platform == "windows":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬇️ Скачать FLClash (Windows)", url=FLCLASH_WINDOWS)],
            [InlineKeyboardButton(text="← Выбрать платформу", callback_data="apps_menu")],
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        ])
    elif platform == "macos":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬇️ FLClash — M1/M2/M3 (arm64)", url=FLCLASH_MACOS_ARM)],
            [InlineKeyboardButton(text="⬇️ FLClash — Intel (amd64)", url=FLCLASH_MACOS_X64)],
            [InlineKeyboardButton(text="← Выбрать платформу", callback_data="apps_menu")],
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        ])
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Выбрать платформу", callback_data="apps_menu")],
            [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
        ])

    await call.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb, disable_web_page_preview=True)
    await call.answer()


# ── Оплата ───────────────────────────────────────────────────────────────────

@dp.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@dp.message(F.successful_payment)
async def successful_payment(msg: Message):
    payment = msg.successful_payment
    payload = payment.invoice_payload  # "vpn:1m:123456"
    parts = payload.split(":")
    if len(parts) != 3 or parts[0] != "vpn":
        return

    plan_key = parts[1]
    tg_id = msg.from_user.id
    plan = config.PLANS.get(plan_key)
    if not plan:
        return

    name, days, stars, stars_str, rub_kopeks, rub_str = plan

    db.record_payment(
        tg_id=tg_id,
        plan_key=plan_key,
        stars=payment.total_amount,
        charge_id=payment.telegram_payment_charge_id,
    )
    db.delete_reminder_events(tg_id)  # продлили → reset напоминаний
    db.log_event(tg_id, "pay_success", {"plan": plan_key, "stars": payment.total_amount})

    await msg.answer("⏳ Создаю подписку...")

    try:
        async with aiohttp.ClientSession() as session:
            mz_name = _assign_mz_username(tg_id, msg.from_user.username, msg.from_user.first_name)
            user_data = await marzban.create_or_extend_user(session, tg_id, days, mz_username=mz_name)

        mz_sub_url = user_data.get("subscription_url", "")
        if mz_sub_url:
            db.set_sub_url(tg_id, mz_sub_url)
        sub_url = _stable_sub_url(tg_id)
        expire_ts = user_data.get("expire")
        if expire_ts:
            expire_str = datetime.fromtimestamp(expire_ts, tz=timezone.utc).strftime("%d.%m.%Y")
        else:
            expire_str = "—"

        await msg.answer(
            f"✅ <b>Готово! Подписка активирована до {expire_str}</b>\n\n"
            f"🔗 Твоя ссылка для подключения:\n"
            f"<code>{sub_url}</code>\n\n"
            "Нажми <b>📲 Открыть в приложении</b> — клиент откроется с готовым импортом.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
                [InlineKeyboardButton(text="📲 Открыть в приложении", url=_install_url(mz_name))],
                [InlineKeyboardButton(text="📖 Установка", callback_data="apps_menu")],
                [InlineKeyboardButton(text="💬 Поддержка", url=config.SUPPORT_LINK)],
            ]),
        )

        # Уведомление админу
        if config.ADMIN_TG_ID:
            await bot.send_message(
                config.ADMIN_TG_ID,
                f"💰 Новая оплата!\n"
                f"Пользователь: @{msg.from_user.username or tg_id} ({tg_id})\n"
                f"Тариф: {name} ({stars} ⭐️)",
            )

    except Exception as e:
        logger.error(f"Failed to create user: {e}")
        await msg.answer(
            "❌ Что-то пошло не так при создании подписки.\n"
            f"Напиши в поддержку — всё исправим: {config.SUPPORT_LINK}",
        )


# ── Admin: /stats ─────────────────────────────────────────────────────────────

def _fmt_pct(num: int, denom: int) -> str:
    if denom == 0:
        return "—"
    return f"{num * 100 // denom}%"


async def _active_subs_from_marzban() -> tuple[int, int, int]:
    """Returns (active, expired, total) as reported by Marzban. Falls back to (-1, -1, -1)."""
    try:
        async with aiohttp.ClientSession() as session:
            users = await marzban.list_all_users(session)
    except Exception as e:
        logger.error(f"marzban.list_all_users failed: {e}")
        return -1, -1, -1

    now_ts = int(datetime.now(timezone.utc).timestamp())
    active = 0
    expired = 0
    for u in users:
        status = u.get("status")
        expire = u.get("expire") or 0
        if status == "active" and (expire == 0 or expire > now_ts):
            active += 1
        else:
            expired += 1
    return active, expired, len(users)


@dp.message(Command("stats"))
async def cmd_stats(msg: Message):
    if msg.from_user.id != config.ADMIN_TG_ID:
        return

    total = db.count_total_users()
    new_1d = db.count_new_users(1)
    new_7d = db.count_new_users(7)
    new_30d = db.count_new_users(30)

    dau = db.count_active_users(1)
    wau = db.count_active_users(7)
    mau = db.count_active_users(30)

    trial_count = db.count_trial_users()
    paying = db.count_paying_users()
    trial_total, trial_converted = db.trial_to_paid_conversion()

    rev_1d = db.revenue_stars(1)
    rev_7d = db.revenue_stars(7)
    rev_30d = db.revenue_stars(30)
    rev_all = db.revenue_stars(None)

    pay_1d = db.payment_count(1)
    pay_7d = db.payment_count(7)
    pay_30d = db.payment_count(30)
    pay_all = db.payment_count(None)

    plans_30d = db.plan_distribution(30)
    plans_all = db.plan_distribution(None)

    # Retention cohorts: users registered ~N days ago, still active in last 1 day.
    d1_cohort, d1_active = db.retention_cohort(1, window_days=1)
    d7_cohort, d7_active = db.retention_cohort(7, window_days=1)
    d30_cohort, d30_active = db.retention_cohort(30, window_days=1)

    # Funnel for last 30d from events.
    funnel = db.event_counts(
        ["start", "trial_activated", "pay_initiated", "pay_success"], days=30
    )

    mz_active, mz_expired, mz_total = await _active_subs_from_marzban()
    mz_line = (
        f"{mz_active} active / {mz_expired} expired (всего в Marzban: {mz_total})"
        if mz_active >= 0 else "❌ Marzban unreachable"
    )

    arpu = (rev_all / total) if total else 0
    arppu = (rev_all / paying) if paying else 0

    plans_30d_str = "\n".join(
        f"  • {k}: {c} шт. ({s} ⭐)" for k, c, s in plans_30d
    ) or "  —"
    plans_all_str = "\n".join(
        f"  • {k}: {c} шт. ({s} ⭐)" for k, c, s in plans_all
    ) or "  —"

    text = (
        "📊 <b>Статистика RadarShield</b>\n\n"
        "<b>Пользователи</b>\n"
        f"  Всего: <b>{total}</b>\n"
        f"  Новые 24ч / 7д / 30д: {new_1d} / {new_7d} / {new_30d}\n"
        f"  DAU / WAU / MAU: <b>{dau}</b> / <b>{wau}</b> / <b>{mau}</b>\n\n"
        "<b>Подписки (Marzban)</b>\n"
        f"  {mz_line}\n\n"
        "<b>Триалы</b>\n"
        f"  Активировали триал: {trial_count}\n"
        f"  Платящих: {paying}\n"
        f"  Trial → paid: {trial_converted}/{trial_total} "
        f"({_fmt_pct(trial_converted, trial_total)})\n\n"
        "<b>Выручка (⭐)</b>\n"
        f"  24ч: {rev_1d} ⭐ ({pay_1d} платежей)\n"
        f"  7д: {rev_7d} ⭐ ({pay_7d})\n"
        f"  30д: {rev_30d} ⭐ ({pay_30d})\n"
        f"  Всего: <b>{rev_all} ⭐</b> ({pay_all})\n"
        f"  ARPU: {arpu:.1f} ⭐ / ARPPU: {arppu:.1f} ⭐\n\n"
        "<b>Планы за 30д</b>\n"
        f"{plans_30d_str}\n\n"
        "<b>Планы за всё время</b>\n"
        f"{plans_all_str}\n\n"
        "<b>Retention (активность в последние 24ч)</b>\n"
        f"  D1: {d1_active}/{d1_cohort} ({_fmt_pct(d1_active, d1_cohort)})\n"
        f"  D7: {d7_active}/{d7_cohort} ({_fmt_pct(d7_active, d7_cohort)})\n"
        f"  D30: {d30_active}/{d30_cohort} ({_fmt_pct(d30_active, d30_cohort)})\n\n"
        "<b>Воронка за 30д</b>\n"
        f"  start: {funnel.get('start', 0)}\n"
        f"  trial_activated: {funnel.get('trial_activated', 0)}\n"
        f"  pay_initiated: {funnel.get('pay_initiated', 0)}\n"
        f"  pay_success: {funnel.get('pay_success', 0)}\n"
    )
    await msg.answer(text, parse_mode=ParseMode.HTML)


# ── Admin: сброс пользователя ─────────────────────────────────────────────────

@dp.message(Command("reset_user"))
async def cmd_reset_user(msg: Message):
    if msg.from_user.id != config.ADMIN_TG_ID:
        return
    parts = msg.text.split()
    if len(parts) != 2 or not parts[1].lstrip("-").isdigit():
        await msg.answer("Использование: /reset_user <tg_id>")
        return
    tg_id = int(parts[1])
    db.delete_user(tg_id)
    await msg.answer(f"✅ Пользователь {tg_id} сброшен — при следующем /start получит пробный период как новый.")


@dp.callback_query(F.data == "do_start")
async def cb_do_start(call: CallbackQuery):
    try:
        await call.message.delete()
    except Exception:
        pass
    await _do_start(call.from_user.id, call.from_user.username, call.from_user.first_name, call.message.answer)
    await call.answer()


# ── Мониторинг здоровья нод Marzban ──────────────────────────────────────────
#
# Enforcement истёкших юзеров делает сам Marzban — он ставит status=expired/limited
# и пушит эти изменения на ноды. Нам enforcement дублировать не нужно.
# Нужно только следить, что все ноды connected — иначе Marzban не сможет доставлять
# изменения, и заблокированные юзеры продолжат пользоваться через зависшую ноду.

NODE_HEALTH_INTERVAL = 3600  # 1 час

_node_alerted: dict[int, bool] = {}


async def _check_nodes_health(session: aiohttp.ClientSession) -> None:
    """Если нода != connected — шлём алерт админу (один раз, пока не починится).
    При восстановлении — recovery-алерт."""
    if not config.ADMIN_TG_ID:
        return
    try:
        headers = await marzban._headers(session)
        resp = await session.get(f"{marzban.MARZBAN_URL}/api/nodes", headers=headers)
        resp.raise_for_status()
        nodes = await resp.json()
    except Exception as e:
        logger.error(f"nodes health check failed: {e}")
        return

    for n in nodes:
        node_id = n.get("id")
        name = n.get("name", f"node#{node_id}")
        status = n.get("status", "unknown")
        was_down = _node_alerted.get(node_id, False)

        if status != "connected" and not was_down:
            _node_alerted[node_id] = True
            try:
                await bot.send_message(
                    config.ADMIN_TG_ID,
                    f"⚠️ <b>Нода Marzban не в порядке</b>\n\n"
                    f"Имя: <b>{name}</b>\n"
                    f"Статус: <code>{status}</code>\n\n"
                    f"Пока нода в этом состоянии — Marzban не может доставлять ей "
                    f"изменения юзеров (disable/revoke). Юзеры с истёкшими подписками "
                    f"могут продолжать пользоваться через эту ноду.\n\n"
                    f"Проверь и при необходимости перезапусти Marzban-панель "
                    f"(`docker restart` контейнера marzban).",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                logger.error(f"failed to send node down alert: {e}")
        elif status == "connected" and was_down:
            _node_alerted[node_id] = False
            try:
                await bot.send_message(
                    config.ADMIN_TG_ID,
                    f"✅ Нода <b>{name}</b> снова connected.",
                    parse_mode=ParseMode.HTML,
                )
            except Exception as e:
                logger.error(f"failed to send node recovery alert: {e}")


async def _node_health_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                await _check_nodes_health(session)
        except Exception as e:
            logger.error(f"node health loop error: {e}")
        await asyncio.sleep(NODE_HEALTH_INTERVAL)


# ── Напоминания об истечении подписки ────────────────────────────────────────
# Проверяем всех пользователей, сравниваем expire из Marzban с сейчас и шлём
# уведомление за 3 дня / 1 день / 2 часа, а также когда подписка истекла.
# События reminder_* пишутся в events, чтобы не дублировать. При продлении
# (record_payment / landing-merge / бонус-день) старые reminder_* очищаются
# и новые напоминания срабатывают в следующий цикл.

EXPIRE_REMINDERS_INTERVAL = 3600  # раз в час

REMINDER_THRESHOLDS_HOURS = [(72, "3d"), (24, "1d"), (2, "2h")]


async def _check_expire_reminders(session: aiohttp.ClientSession) -> int:
    """Проходит users.mz_username, шлёт напоминания. Возвращает кол-во отправленных."""
    users = db.get_users_with_mz()
    now_ts = int(datetime.now(timezone.utc).timestamp())
    sent = 0

    for u in users:
        tg_id = u["tg_id"]
        mz = u["mz_username"]
        try:
            mz_user = await marzban.get_user(session, 0, mz_username=mz)
        except Exception as e:
            logger.error(f"reminder: get_user {mz} failed: {e}")
            continue
        if not mz_user:
            continue

        expire = mz_user.get("expire") or 0
        status = mz_user.get("status")
        if not expire:
            # Бессрочная подписка (безлимит) — напоминания не нужны
            continue

        hours_left = (expire - now_ts) / 3600

        # Подписка свежая / продлена — сбрасываем все reminder-события, чтобы
        # напоминания сработали снова когда новый срок подойдёт к концу.
        if hours_left > 72 + 1:
            db.delete_reminder_events(tg_id)
            continue

        # Ищем ближайший порог ≥ текущего остатка, по которому ещё не шлём
        target = None  # (hours, label)
        for hrs, label in REMINDER_THRESHOLDS_HOURS:
            if hours_left <= hrs and db.count_events(tg_id, f"reminder_{label}") == 0:
                target = (hrs, label)
                break

        if target is None and hours_left <= 0 and db.count_events(tg_id, "reminder_expired") == 0:
            # Подписка истекла — отдельное напоминание
            target = (0, "expired")

        if target is None:
            continue

        hrs, label = target
        try:
            await _send_expire_reminder(tg_id, hours_left, label)
            db.log_event(tg_id, f"reminder_{label}", {"expire": expire})
            sent += 1
        except Exception as e:
            logger.error(f"reminder send to {tg_id} failed: {e}")

    return sent


async def _send_expire_reminder(tg_id: int, hours_left: float, label: str) -> None:
    mz_name = db.get_mz_username(tg_id)
    install_btn = (
        [InlineKeyboardButton(text="📲 Открыть в приложении", url=_install_url(mz_name))]
        if mz_name else []
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Продлить подписку", url=_pay_url(tg_id))],
        *( [install_btn] if install_btn else [] ),
        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
    ])

    if label == "expired":
        text = (
            "❌ <b>Твоя подписка истекла.</b>\n\n"
            "VPN больше не работает. Продли подписку одним кликом — "
            "ссылка в клиенте переживёт и станет активной снова."
        )
    elif label == "2h":
        hrs_int = max(1, int(hours_left))
        text = (
            f"⏰ <b>Подписка истечёт через ~{hrs_int} ч.</b>\n\n"
            "Продли сейчас чтобы не терять доступ."
        )
    elif label == "1d":
        text = (
            "⏰ <b>Подписка истечёт через 1 день.</b>\n\n"
            "Продли заранее — займёт 30 секунд."
        )
    else:  # "3d"
        text = (
            "📅 <b>Подписка истечёт через 3 дня.</b>\n\n"
            "Продли чтобы не потерять доступ."
        )

    await bot.send_message(tg_id, text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def _expire_reminders_loop():
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                sent = await _check_expire_reminders(session)
            if sent:
                logger.info(f"expire reminders sent: {sent}")
        except Exception as e:
            logger.error(f"expire reminders loop error: {e}")
        await asyncio.sleep(EXPIRE_REMINDERS_INTERVAL)


@dp.message(Command("nodes"))
async def cmd_nodes(msg: Message):
    """Показывает статус всех нод Marzban — только для админа."""
    if msg.from_user.id != config.ADMIN_TG_ID:
        return
    try:
        async with aiohttp.ClientSession() as session:
            headers = await marzban._headers(session)
            resp = await session.get(f"{marzban.MARZBAN_URL}/api/nodes", headers=headers)
            resp.raise_for_status()
            nodes = await resp.json()
        lines = ["📡 <b>Статус нод Marzban</b>\n"]
        for n in nodes:
            emoji = "✅" if n.get("status") == "connected" else "⚠️"
            lines.append(
                f"{emoji} <b>{n.get('name')}</b> — {n.get('status')} "
                f"(xray {n.get('xray_version') or '—'})"
            )
        await msg.answer("\n".join(lines), parse_mode=ParseMode.HTML)
    except Exception as e:
        await msg.answer(f"❌ Ошибка: {e}")


# ── Catch-all: незарегистрированные пользователи (ПОСЛЕ всех команд!) ────────

@dp.message()
async def handle_unknown(msg: Message):
    if not msg.from_user:
        return
    if not db.user_exists(msg.from_user.id):
        await msg.answer(
            "👋 Привет! Нажми кнопку ниже — активирую бесплатный период на 7 дней и 5 ГБ.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🚀 Начать", callback_data="do_start")]
            ]),
        )


# ── main ─────────────────────────────────────────────────────────────────────

async def main():
    db.init_db()
    logger.info("Bot starting...")
    asyncio.create_task(_node_health_loop())
    asyncio.create_task(_expire_reminders_loop())
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
