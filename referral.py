"""Рефералка RadarShield — начисление бонуса при ПЕРВОЙ оплате приглашённого.

Общий модуль для обоих рельсов оплаты: Stars (в боте) и Robokassa (в лендинге).
Оба после успешной активации подписки зовут `credit_first_payment(tg_id)`.

Награда — бонус-ДНИ (не скидка): дни бесплатны, оплаты/фискалки не касаются
(решение по промокодам 22.07). Начисляется РОВНО на первой оплате приглашённого и
один раз (`referral_credited`) — фейки не платят, так что это анти-фарм.

Уведомления шлём прямым Bot API (aiohttp), чтобы модуль работал и в боте, и в
лендинге, где aiogram-инстанса нет.
"""
import logging
import os

import aiohttp

import db
import panel

logger = logging.getLogger(__name__)

INVITED_DAYS = 7    # другу (приглашённому) — при его первой оплате
INVITER_DAYS = 7    # пригласившему — за оплатившего друга

_API = "https://api.telegram.org/bot{token}/sendMessage"


async def _notify(tg_id: int, text: str) -> None:
    """Тихо уведомить юзера. Ошибка доставки не должна ронять начисление."""
    token = os.environ.get("BOT_TOKEN", "")
    if not token:
        return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                _API.format(token=token),
                data={"chat_id": tg_id, "text": text, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=15),
            )
    except Exception as e:
        logger.warning(f"referral notify {tg_id} failed: {e}")


async def credit_first_payment(tg_id: int) -> dict | None:
    """Начислить реф-бонус, если это первая оплата приглашённого и ещё не начисляли.

    Зовётся ПОСЛЕ активации подписки (add_bonus_days требует существующего юзера
    панели с активным сроком). Идемпотентно: `referral_credited` не даёт задвоить.
    → {"inviter", "invited", "days"} при начислении, иначе None.
    """
    if db.is_referral_credited(tg_id):
        return None
    referrer = db.get_referrer(tg_id)
    if not referrer:
        return None
    # Строго ПЕРВАЯ оплата: зовут сразу после record_payment, значит count==1.
    if len(db.get_user_payments(tg_id)) != 1:
        return None

    invited_mz = db.get_mz_username(tg_id)
    inviter_mz = db.get_mz_username(referrer)

    # Приглашённому — дни сверх только что оплаченной подписки. Это ядро награды:
    # не вышло начислить — не помечаем credited (пусть попробуется при доплате).
    try:
        await panel.add_bonus_days(invited_mz, INVITED_DAYS)
    except Exception as e:
        logger.error(f"referral: invited grant {tg_id} ({invited_mz}) failed: {e}")
        return None

    # Пригласившему — best-effort: у него может не быть активной подписки/аккаунта
    # в панели (звал, но сам не подключался). Награду друга это блокировать не должно.
    inviter_ok = False
    if inviter_mz:
        try:
            await panel.add_bonus_days(inviter_mz, INVITER_DAYS)
            inviter_ok = True
        except Exception as e:
            logger.warning(f"referral: inviter grant {referrer} ({inviter_mz}) failed: {e}")

    db.mark_referral_credited(tg_id)
    # Продлили сроки → сбрасываем reminder-события, чтобы напоминания сработали заново.
    db.delete_reminder_events(tg_id)
    if inviter_ok:
        db.delete_reminder_events(referrer)

    await _notify(
        tg_id,
        f"🎁 <b>Тебе +{INVITED_DAYS} дней!</b>\n\n"
        "Ты пришёл по приглашению друга — начислили бонусные дни сверх твоей "
        "подписки. Спасибо, что с нами!",
    )
    if inviter_ok:
        await _notify(
            referrer,
            f"🎉 <b>Твой друг оформил подписку — тебе +{INVITER_DAYS} дней!</b>\n\n"
            "Спасибо, что зовёшь друзей в RadarShield. Продолжай приглашать — "
            "бонусные дни за каждого, кто оплатит.",
        )

    logger.info(f"referral credited: invited={tg_id} inviter={referrer} inviter_ok={inviter_ok}")
    return {"inviter": referrer, "invited": tg_id, "days": INVITED_DAYS}
