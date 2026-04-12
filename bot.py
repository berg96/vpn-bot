import asyncio
import logging
import aiohttp
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    LabeledPrice, PreCheckoutQuery,
)
from aiogram.filters import CommandStart, Command
from aiogram.enums import ParseMode

import config
import db
import marzban


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


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()


# ── Клавиатуры ──────────────────────────────────────────────────────────────

def plans_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for key, (name, days, stars, price_str) in config.PLANS.items():
        buttons.append([InlineKeyboardButton(
            text=f"{name} — {price_str}",
            callback_data=f"buy:{key}",
        )])
    buttons.append([
        InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
        InlineKeyboardButton(text="📲 Установка", callback_data="apps_menu"),
    ])
    buttons.append([InlineKeyboardButton(text="💬 Поддержка", url=config.SUPPORT_LINK)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📲 Установка", callback_data="apps_menu")],
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
        "3. В Karing: нажми <b>+</b> → <b>Import from clipboard</b> (вставит автоматически)\n\n"
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
    db.record_user(tg_id, username)

    if not db.is_trial_used(tg_id):
        await answer_fn(
            "🔐 <b>RadarShield VPN</b>\n\n"
            "⏳ Активирую пробный период...",
            parse_mode=ParseMode.HTML,
        )
        try:
            async with aiohttp.ClientSession() as session:
                mz_name = _assign_mz_username(tg_id, username, first_name)
                user_data = await marzban.create_trial_user(session, tg_id, days=10, data_limit_gb=5.0, mz_username=mz_name)
            db.mark_trial_used(tg_id)
            sub_url = user_data.get("subscription_url", "")
            if sub_url:
                db.set_sub_url(tg_id, sub_url)
            await answer_fn(
                "🎁 <b>Тебе активирован бесплатный период на 10 дней (5 ГБ)</b>\n\n"
                "🔗 Ссылка для подключения:\n"
                f"<code>{sub_url}</code>\n\n"
                "📱 <b>Как подключиться:</b>\n"
                "• <b>Android / Windows / macOS</b> → FLClash\n"
                "• <b>iOS</b> → Karing\n\n"
                "Нажми <b>Установка</b> — покажу как подключиться 👇",
                parse_mode=ParseMode.HTML,
                reply_markup=start_keyboard(),
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
async def cmd_start(msg: Message):
    await _do_start(msg.from_user.id, msg.from_user.username, msg.from_user.first_name, msg.answer)


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
            text = "👋 Нажми кнопку — активирую бесплатный период на 10 дней."
        else:
            text = "У тебя пока нет активной подписки.\n\nВыбери тариф ниже 👇"
            kb = plans_keyboard()
    else:
        status = user.get("status", "unknown")
        expire_ts = user.get("expire")
        if expire_ts:
            expire_dt = datetime.fromtimestamp(expire_ts, tz=timezone.utc)
            expire_str = expire_dt.strftime("%d.%m.%Y")
            days_left = (expire_dt - datetime.now(timezone.utc)).days
            expire_line = f"📅 Истекает: <b>{expire_str}</b> (через {days_left} дн.)"
        else:
            expire_line = "📅 Срок: <b>бессрочно</b>"

        sub_url = db.get_sub_url(tg_id) or user.get("subscription_url", "")
        if sub_url and not db.get_sub_url(tg_id):
            db.set_sub_url(tg_id, sub_url)
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
            [InlineKeyboardButton(text="📲 Установка", callback_data="apps_menu")],
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

    name, days, stars, price_str = plan
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
        reply_markup=plans_keyboard(),
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
                "👋 Нажми кнопку — активирую бесплатный период на 10 дней.",
                reply_markup=kb,
            )
            await event.answer()
        else:
            await event.answer(
                "👋 Нажми кнопку — активирую бесплатный период на 10 дней.",
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
            [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
        ])
    elif platform == "windows":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬇️ Скачать FLClash (Windows)", url=FLCLASH_WINDOWS)],
            [InlineKeyboardButton(text="← Выбрать платформу", callback_data="apps_menu")],
            [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
        ])
    elif platform == "macos":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬇️ FLClash — M1/M2/M3 (arm64)", url=FLCLASH_MACOS_ARM)],
            [InlineKeyboardButton(text="⬇️ FLClash — Intel (amd64)", url=FLCLASH_MACOS_X64)],
            [InlineKeyboardButton(text="← Выбрать платформу", callback_data="apps_menu")],
            [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
        ])
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Выбрать платформу", callback_data="apps_menu")],
            [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
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

    name, days, stars, price_str = plan

    db.record_payment(
        tg_id=tg_id,
        plan_key=plan_key,
        stars=payment.total_amount,
        charge_id=payment.telegram_payment_charge_id,
    )

    await msg.answer("⏳ Создаю подписку...")

    try:
        async with aiohttp.ClientSession() as session:
            mz_name = _assign_mz_username(tg_id, msg.from_user.username, msg.from_user.first_name)
            user_data = await marzban.create_or_extend_user(session, tg_id, days, mz_username=mz_name)

        sub_url = user_data.get("subscription_url", "")
        if sub_url:
            db.set_sub_url(tg_id, sub_url)
        expire_ts = user_data.get("expire")
        if expire_ts:
            expire_str = datetime.fromtimestamp(expire_ts, tz=timezone.utc).strftime("%d.%m.%Y")
        else:
            expire_str = "—"

        await msg.answer(
            f"✅ <b>Готово! Подписка активирована до {expire_str}</b>\n\n"
            f"🔗 Твоя ссылка для подключения:\n"
            f"<code>{sub_url}</code>\n\n"
            "📱 <b>Как подключиться:</b>\n"
            "• <b>Android / Windows / macOS</b> → FLClash: + → Import from URL\n"
            "• <b>iOS</b> → Karing: + → Import from URL\n\n"
            "Ссылка обновляется автоматически — сохрани её один раз в приложение.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 Мой профиль", callback_data="profile")],
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
    await msg.answer(f"✅ Пользователь {tg_id} сброшен — при следующем /start получит триал как новый.")


# ── Catch-all: незарегистрированные пользователи ──────────────────────────────

@dp.message()
async def handle_unknown(msg: Message):
    if not msg.from_user:
        return
    if not db.user_exists(msg.from_user.id):
        await msg.answer(
            "👋 Привет! Нажми кнопку ниже — активирую бесплатный период на 10 дней.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🚀 Начать", callback_data="do_start")]
            ]),
        )


@dp.callback_query(F.data == "do_start")
async def cb_do_start(call: CallbackQuery):
    try:
        await call.message.delete()
    except Exception:
        pass
    await _do_start(call.from_user.id, call.from_user.username, call.from_user.first_name, call.message.answer)
    await call.answer()


# ── main ─────────────────────────────────────────────────────────────────────

async def main():
    db.init_db()
    logger.info("Bot starting...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
