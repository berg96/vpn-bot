"""Robokassa acquiring API.

Поток:
  pay.html → /api/robokassa/init (создаём pending в БД, формируем URL формы)
  → редирект юзера на auth.robokassa.ru → юзер платит
  → Robokassa POST на /api/robokassa/result → verify подписи → активация
  → юзер редиректится на /pay/success с подписью.

Подписи (всё SHA256, lowercase hex):
  Init   : SHA256(MerchantLogin:OutSum:InvId:Receipt:Password1[:Shp_*])
  Result : SHA256(OutSum:InvId:Password2[:Shp_*])
  Success: SHA256(OutSum:InvId:Password1[:Shp_*])

Shp_-параметры в подписи — отсортированы по имени, формат "Shp_key=value".
Receipt в подписи — сырой JSON (минимизированный, без URL-кодирования).
В URL Receipt уходит rawurlencoded — query собираем вручную, чтобы urlencode
не закодировал его второй раз.

Опытным путём (тесты на radarshield в IsTest=1, апрель 2026): варианты с
URL-encoded Receipt в подписи давали Error 29, raw JSON — проходил.
Официальный PHP-пример Robokassa с `$receipt = "%7B..."` оказался ошибочным
для SHA256-режима либо устарел.
"""
import hashlib
import json
import logging
import os
import urllib.parse
from typing import Any

import aiohttp

PAYMENT_URL = "https://auth.robokassa.ru/Merchant/Index.aspx"

MERCHANT_LOGIN = os.environ["ROBOKASSA_MERCHANT_LOGIN"]
TEST_MODE = os.environ.get("ROBOKASSA_TEST_MODE", "0") == "1"

if TEST_MODE:
    PASSWORD_1 = os.environ["ROBOKASSA_TEST_PASSWORD_1"]
    PASSWORD_2 = os.environ["ROBOKASSA_TEST_PASSWORD_2"]
else:
    PASSWORD_1 = os.environ["ROBOKASSA_PASSWORD_1"]
    PASSWORD_2 = os.environ["ROBOKASSA_PASSWORD_2"]

# ФЗ-54 defaults — те же, что использовали в tinkoff.py
RECEIPT_TAXATION = os.environ.get("RECEIPT_TAXATION", "usn_income")
RECEIPT_TAX = os.environ.get("RECEIPT_TAX", "none")
RECEIPT_PAYMENT_METHOD = os.environ.get("RECEIPT_PAYMENT_METHOD", "full_prepayment")
RECEIPT_PAYMENT_OBJECT = os.environ.get("RECEIPT_PAYMENT_OBJECT", "service")
RECEIPT_DEFAULT_EMAIL = os.environ.get("RECEIPT_DEFAULT_EMAIL", "noreply@radarshield.mooo.com")
RECEIPT_ITEM_NAME_PREFIX = os.environ.get(
    "RECEIPT_ITEM_NAME_PREFIX", "Подписка на сервис RadarShield"
)

logger = logging.getLogger(__name__)


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _shp_suffix(shp: dict[str, str]) -> str:
    if not shp:
        return ""
    parts = [f"{k}={v}" for k, v in sorted(shp.items())]
    return ":" + ":".join(parts)


def build_receipt(item_name: str, amount_rub: float) -> dict[str, Any]:
    return {
        "sno": RECEIPT_TAXATION,
        "items": [{
            "name": item_name[:128],
            "quantity": 1,
            "sum": round(amount_rub, 2),
            "tax": RECEIPT_TAX,
            "payment_method": RECEIPT_PAYMENT_METHOD,
            "payment_object": RECEIPT_PAYMENT_OBJECT,
        }],
    }


def make_payment_url(
    *,
    inv_id: int,
    amount_rub: float,
    description: str,
    receipt: dict[str, Any],
    customer_email: str,
    shp: dict[str, str],
) -> str:
    out_sum = f"{amount_rub:.2f}"
    receipt_json = json.dumps(receipt, ensure_ascii=False, separators=(",", ":"))
    receipt_encoded = urllib.parse.quote(receipt_json, safe="")

    sig_str = (
        f"{MERCHANT_LOGIN}:{out_sum}:{inv_id}:{receipt_json}:{PASSWORD_1}"
        + _shp_suffix(shp)
    )
    signature = _sha256(sig_str)

    params = [
        ("MerchantLogin", MERCHANT_LOGIN),
        ("OutSum", out_sum),
        ("InvId", str(inv_id)),
        ("Description", description[:100]),
        ("SignatureValue", signature),
        ("Email", customer_email),
        ("Culture", "ru"),
        ("Encoding", "utf-8"),
    ]
    if TEST_MODE:
        params.append(("IsTest", "1"))
    for k, v in sorted(shp.items()):
        params.append((k, v))

    qs_parts = [
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in params
    ]
    qs_parts.append(f"Receipt={receipt_encoded}")
    return PAYMENT_URL + "?" + "&".join(qs_parts)


def verify_result(out_sum: str, inv_id: str, signature: str, shp: dict[str, str]) -> bool:
    """Подпись webhook ResultURL. Использует Password2."""
    expected = _sha256(f"{out_sum}:{inv_id}:{PASSWORD_2}" + _shp_suffix(shp))
    return expected.lower() == signature.lower()


def verify_success(out_sum: str, inv_id: str, signature: str, shp: dict[str, str]) -> bool:
    """Подпись возврата на SuccessURL. Использует Password1."""
    expected = _sha256(f"{out_sum}:{inv_id}:{PASSWORD_1}" + _shp_suffix(shp))
    return expected.lower() == signature.lower()


async def health_check(session: aiohttp.ClientSession) -> bool:
    """Доступность сервиса. Не различает 'магазин блокирован' и 'хост лежит' —
    блокировку магазина увидим по реальным Init-ошибкам в логах."""
    try:
        async with session.get(PAYMENT_URL, timeout=aiohttp.ClientTimeout(total=5)) as r:
            return r.status < 500
    except Exception as e:
        logger.error(f"robokassa health_check: {e}")
        return False
