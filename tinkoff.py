"""Tinkoff Acquiring API (E-commerce, v2) — приём платежей картой.

Схема на DEMO-терминале:
  bot → Init → PaymentURL → юзер → оплачивает на securepay.tinkoff.ru
  → Т-Банк шлёт notification на NotificationURL → мы активируем подписку.

Когда будет боевой терминал + провайдер-токен в BotFather —
переключим на Telegram Bot Payments (нативный UI), оставив этот модуль
как fallback для pay-by-link сценариев.
"""
import hashlib
import logging
import os
from typing import Any

import aiohttp

BASE_URL = "https://securepay.tinkoff.ru/v2"

TERMINAL_KEY = os.environ["TINKOFF_TERMINAL_KEY"]
PASSWORD = os.environ["TINKOFF_PASSWORD"]
NOTIFICATION_URL = os.environ.get("TINKOFF_NOTIFICATION_URL", "")
SUCCESS_URL = os.environ.get("TINKOFF_SUCCESS_URL", "")
FAIL_URL = os.environ.get("TINKOFF_FAIL_URL", "")

# ФЗ-54 defaults (корректируются через env после разговора с бухгалтером)
RECEIPT_TAXATION = os.environ.get("RECEIPT_TAXATION", "usn_income")  # УСН Доходы 6%
RECEIPT_TAX = os.environ.get("RECEIPT_TAX", "none")                  # ИП на УСН без НДС
RECEIPT_PAYMENT_METHOD = os.environ.get("RECEIPT_PAYMENT_METHOD", "full_prepayment")
RECEIPT_PAYMENT_OBJECT = os.environ.get("RECEIPT_PAYMENT_OBJECT", "service")
RECEIPT_DEFAULT_EMAIL = os.environ.get("RECEIPT_DEFAULT_EMAIL", "noreply@radarshield.mooo.com")

# Название позиции в чеке ФНС и описание платежа в банковской выписке.
# Избегаем слов "VPN", "обход блокировок" — могут вызвать вопросы от банка/РКН.
# Формулировка нейтрально-правовая: подписка на программный сервис.
RECEIPT_ITEM_NAME_PREFIX = os.environ.get(
    "RECEIPT_ITEM_NAME_PREFIX", "Подписка на сервис RadarShield"
)

logger = logging.getLogger(__name__)


def _make_token(payload: dict[str, Any]) -> str:
    """Token = SHA-256 от конкатенации отсортированных по ключу значений
    top-level полей + TerminalKey + Password. Вложенные объекты (Receipt, DATA)
    в подпись не включаются.
    """
    values_map: dict[str, str] = {}
    for k, v in payload.items():
        if isinstance(v, (dict, list)):
            continue  # Receipt, DATA и прочие вложенные — не подписываем
        if v is None:
            continue
        if isinstance(v, bool):
            values_map[k] = "true" if v else "false"
        else:
            values_map[k] = str(v)
    values_map["TerminalKey"] = TERMINAL_KEY
    values_map["Password"] = PASSWORD

    concat = "".join(values_map[k] for k in sorted(values_map.keys()))
    return hashlib.sha256(concat.encode("utf-8")).hexdigest()


def verify_notification(payload: dict[str, Any]) -> bool:
    """Проверяем Token из webhook Т-Банка. Тот же алгоритм, только уже с полученным Token."""
    received = payload.get("Token")
    if not received:
        return False
    body = {k: v for k, v in payload.items() if k != "Token"}
    expected = _make_token(body)
    return received == expected


def _build_receipt(
    item_name: str,
    amount_kopeks: int,
    customer_email: str | None,
    customer_phone: str | None = None,
) -> dict[str, Any]:
    """Формирует Receipt-объект ФЗ-54 для Т-Банка.

    Т-Банк требует минимум один из Email/Phone у покупателя.
    Обязательные параметры (конкретика зависит от СНО — уточняется у бухгалтера):
      - Taxation: система налогообложения (usn_income для УСН Доходы 6%)
      - Items[].Tax: ставка НДС (none для ИП без НДС)
      - Items[].PaymentMethod: способ расчёта (full_prepayment — подписка с предоплатой)
      - Items[].PaymentObject: предмет расчёта (service — услуга)
    """
    receipt: dict[str, Any] = {
        "Taxation": RECEIPT_TAXATION,
        "Items": [
            {
                "Name": item_name[:128],
                "Price": amount_kopeks,
                "Quantity": 1,
                "Amount": amount_kopeks,
                "Tax": RECEIPT_TAX,
                "PaymentMethod": RECEIPT_PAYMENT_METHOD,
                "PaymentObject": RECEIPT_PAYMENT_OBJECT,
            }
        ],
    }
    email = customer_email or RECEIPT_DEFAULT_EMAIL
    if email:
        receipt["Email"] = email
    if customer_phone:
        receipt["Phone"] = customer_phone
    return receipt


async def init_payment(
    session: aiohttp.ClientSession,
    *,
    amount_kopeks: int,
    order_id: str,
    description: str,
    customer_key: str | None = None,
    customer_email: str | None = None,
    customer_phone: str | None = None,
) -> dict[str, Any]:
    """Создаёт платёж. Возвращает dict с PaymentId, PaymentURL, Status.

    amount_kopeks: сумма в копейках (int).
    order_id: уникальный OrderId на нашей стороне (строка).
    description: короткое описание товара (до ~250 символов), оно же Name позиции чека.
    customer_key: стабильный ID клиента (tg_id) — для рекуррентных платежей.
    customer_email/phone: для отправки чека покупателю (хотя бы один обязателен ФЗ-54).
    """
    payload: dict[str, Any] = {
        "TerminalKey": TERMINAL_KEY,
        "Amount": amount_kopeks,
        "OrderId": order_id,
        "Description": description[:250],
    }
    if customer_key:
        payload["CustomerKey"] = customer_key
    if NOTIFICATION_URL:
        payload["NotificationURL"] = NOTIFICATION_URL
    if SUCCESS_URL:
        payload["SuccessURL"] = SUCCESS_URL
    if FAIL_URL:
        payload["FailURL"] = FAIL_URL

    # Receipt (ФЗ-54). В подпись Token вложенные объекты не входят — см. _make_token.
    payload["Receipt"] = _build_receipt(
        item_name=description,
        amount_kopeks=amount_kopeks,
        customer_email=customer_email,
        customer_phone=customer_phone,
    )

    payload["Token"] = _make_token(payload)

    async with session.post(f"{BASE_URL}/Init", json=payload) as resp:
        data = await resp.json()

    if not data.get("Success"):
        logger.error(f"Tinkoff Init failed: {data}")
        raise RuntimeError(
            f"Tinkoff error {data.get('ErrorCode')}: {data.get('Message') or data.get('Details')}"
        )

    return data


async def get_state(session: aiohttp.ClientSession, payment_id: str) -> dict[str, Any]:
    """Проверка статуса платежа (для recovery если webhook пропал)."""
    payload = {
        "TerminalKey": TERMINAL_KEY,
        "PaymentId": payment_id,
    }
    payload["Token"] = _make_token(payload)
    async with session.post(f"{BASE_URL}/GetState", json=payload) as resp:
        return await resp.json()
