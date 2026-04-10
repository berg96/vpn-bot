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


def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="← Назад", callback_data="back_to_plans")]
    ])


APPS_TEXT = {
    "android": (
        "📱 <b>Android — Hiddify</b>\n\n"
        "1. Установи <a href='https://play.google.com/store/apps/details?id=app.hiddify.com'>Hiddify</a> из Google Play\n"
        "   или <a href='https://github.com/hiddify/hiddify-app/releases'>скачай APK с GitHub</a>\n\n"
        "2. Открой бота → /profile → скопируй ссылку\n\n"
        "3. В Hiddify: нажми <b>+</b> → <b>Добавить из буфера обмена</b>\n\n"
        "4. Нажми кнопку подключения\n\n"
        "✅ Готово!"
    ),
    "ios": (
        "📱 <b>iOS — Streisand</b>\n\n"
        "1. Установи <a href='https://apps.apple.com/app/streisand/id6450534064'>Streisand</a> из App Store\n\n"
        "2. Открой бота → /profile → скопируй ссылку\n\n"
        "3. В Streisand: нажми <b>+</b> → <b>Импорт из буфера</b>\n\n"
        "4. Нажми на конфигурацию → <b>Подключить</b>\n\n"
        "✅ Готово! Разреши добавление VPN-профиля при первом запуске."
    ),
    "windows": (
        "💻 <b>Windows — Hiddify</b>\n\n"
        "1. Скачай <a href='https://github.com/hiddify/hiddify-app/releases'>Hiddify с GitHub</a> (файл Hiddify-Windows-Setup-x64.exe)\n\n"
        "2. Установи и запусти\n\n"
        "3. Открой бота → /profile → скопируй ссылку\n\n"
        "4. В Hiddify: нажми <b>+</b> → <b>Добавить из буфера обмена</b>\n\n"
        "5. Нажми кнопку подключения\n\n"
        "✅ Готово!"
    ),
    "macos": (
        "💻 <b>macOS — Hiddify</b>\n\n"
        "1. Скачай <a href='https://github.com/hiddify/hiddify-app/releases'>Hiddify с GitHub</a> (файл Hiddify-MacOS.dmg)\n\n"
        "2. Установи и запусти\n\n"
        "3. Открой бота → /profile → скопируй ссылку\n\n"
        "4. В Hiddify: нажми <b>+</b> → <b>Добавить из буфера обмена</b>\n\n"
        "5. Нажми кнопку подключения\n\n"
        "✅ Готово!"
    ),
    "routing": (
        "🔧 <b>Умная маршрутизация</b>\n\n"
        "Российские сайты будут открываться напрямую — без VPN и без потери скорости.\n"
        "Всё остальное по-прежнему идёт через VPN.\n\n"
        "<b>Как подключить (Hiddify):</b>\n"
        "Нажми кнопку ниже — Hiddify откроется и предложит добавить правила.\n\n"
        "<b>Вручную:</b> Settings → Routing → + → Remote → вставь ссылку:\n"
        "<code>http://82.38.171.220/hiddify-routing.yaml</code>"
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
        [InlineKeyboardButton(text="← Главное меню", callback_data="back_to_plans")],
    ])


# ── /start ───────────────────────────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(msg: Message):
    tg_id = msg.from_user.id
    db.record_user(tg_id, msg.from_user.username)

    # Выдаём триал если ещё не использован
    if not db.is_trial_used(tg_id):
        await msg.answer(
            "🔐 <b>VPN — быстро, надёжно, без лишних вопросов</b>\n\n"
            "VLESS Reality — один из самых стойких протоколов к блокировкам.\n\n"
            "⏳ Активирую пробный период...",
            parse_mode=ParseMode.HTML,
        )
        try:
            async with aiohttp.ClientSession() as session:
                mz_name = _assign_mz_username(tg_id, msg.from_user.username, msg.from_user.first_name)
                user_data = await marzban.create_trial_user(session, tg_id, days=10, data_limit_gb=5.0, mz_username=mz_name)
            db.mark_trial_used(tg_id)
            sub_url = user_data.get("subscription_url", "")
            await msg.answer(
                "🎁 <b>Тебе активирован бесплатный период на 10 дней (5 ГБ)</b>\n\n"
                "🔗 Ссылка для подключения:\n"
                f"<code>{sub_url}</code>\n\n"
                "📱 <b>Как подключиться:</b>\n"
                "• <b>Android / Windows / macOS</b> → Hiddify\n"
                "• <b>iOS</b> → Streisand\n\n"
                "После триала выбери тариф для продолжения 👇",
                parse_mode=ParseMode.HTML,
                reply_markup=plans_keyboard(),
            )
        except Exception as e:
            logger.error(f"Trial creation failed for {tg_id}: {e}")
            await msg.answer(
                "🔐 <b>VPN — быстро, надёжно, без лишних вопросов</b>\n\n"
                "Выбери тариф:",
                parse_mode=ParseMode.HTML,
                reply_markup=plans_keyboard(),
            )
    else:
        await msg.answer(
            "🔐 <b>VPN — быстро, надёжно, без лишних вопросов</b>\n\n"
            "VLESS Reality — один из самых стойких протоколов к блокировкам.\n"
            "Работает на Android, iOS, Windows, macOS.\n\n"
            "Выбери тариф:",
            parse_mode=ParseMode.HTML,
            reply_markup=plans_keyboard(),
        )


# ── /profile ─────────────────────────────────────────────────────────────────

@dp.message(Command("profile"))
@dp.callback_query(F.data == "profile")
async def cmd_profile(event: Message | CallbackQuery):
    msg = event.message if isinstance(event, CallbackQuery) else event
    tg_id = event.from_user.id

    mz_name = _resolve_mz_username(tg_id, event.from_user.username, event.from_user.first_name)
    async with aiohttp.ClientSession() as session:
        user = await marzban.get_user(session, tg_id, mz_name)

    if not user:
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

        sub_url = user.get("subscription_url", "")
        status_emoji = {"active": "✅", "expired": "❌", "disabled": "🚫"}.get(status, "❓")

        text = (
            f"{status_emoji} Статус: <b>{status}</b>\n"
            f"{expire_line}\n\n"
            f"🔗 Ссылка для подключения:\n<code>{sub_url}</code>\n\n"
            "Импортируй ссылку в приложение: v2rayNG (Android), "
            "Streisand (iOS), v2rayN (Windows)."
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Продлить", callback_data="back_to_plans")],
            [InlineKeyboardButton(text="📲 Установка", callback_data="apps_menu")],
            [InlineKeyboardButton(text="💬 Поддержка", url=config.SUPPORT_LINK)],
        ])

    if isinstance(event, CallbackQuery):
        await event.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
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
            [InlineKeyboardButton(
                text="🔧 Добавить в Hiddify",
                url="hiddify://install-config/http%3A%2F%2F82.38.171.220%2Fhiddify-routing.yaml",
            )],
            [InlineKeyboardButton(text="← Назад", callback_data="apps_menu")],
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
            "• <b>Android / Windows / macOS</b> → Hiddify: + → из буфера\n"
            "• <b>iOS</b> → Streisand: + → из буфера\n\n"
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
            "👋 Привет! Напиши /start чтобы начать.",
            parse_mode=ParseMode.HTML,
        )


# ── main ─────────────────────────────────────────────────────────────────────

async def main():
    db.init_db()
    logger.info("Bot starting...")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
