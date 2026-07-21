"""Движок кампаний: рассылки, которые не выжигают базу.

Правила зашиты в код, а не в дисциплину отправляющего:

* **Кап.** Маркетинговое сообщение — не чаще 1 раза в 30 дней на юзера. Все
  маркетинговые кампании конкурируют за этот слот. Транзакционные (истечение
  подписки, оплата, инцидент, «обнови профиль — иначе перестанет работать») под
  кап не попадают: их ждут.
  Основание: даже 1 пуш в неделю → 10% отключают уведомления (Localytics/Airship).
  В Telegram цена ошибки выше, чем в email: блок бота = 403 навсегда, а это наш
  канал доставки конфигов и поддержки. Заблокировавший платящий не узнает об
  истечении подписки → не продлит. Блок стоит нам выручки, а не контакта.
* **403 → замолкаем, но не навсегда.** `bot_blocked=1` + время; юзер выпадает из
  выборок на `db.BLOCK_RETRY_DAYS`, потом снова пробуем (мог разблокировать). Дошло —
  блок снимается.
* **429 → ждём `retry_after`** (единственный корректный способ, Bot API).
* **Троттлинг 20 msg/s** — запас к лимиту Telegram (30/s).
* **Выход мягче блока.** У каждого маркетингового сообщения — кнопка «Реже писать».

Транспорт — прямой Bot API через requests, без aiogram: движок должен работать
и внутри бота, и с хоста (раннеру нужен docker-доступ к старой панели).
"""
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Callable

import requests

import db

logger = logging.getLogger(__name__)

RATE_LIMIT_PER_SEC = 20      # запас к лимиту Telegram (30/s)
MARKETING_CAP_DAYS = 30      # не чаще 1 маркетингового в месяц на юзера
MAX_RETRIES = 3

OPT_OUT_CALLBACK = "notify_less"
API = "https://api.telegram.org/bot{token}/sendMessage"


@dataclass
class Campaign:
    """Одна кампания.

    kind='transactional' — обходит месячный кап (истечение, оплата, инцидент).
    once=True — уходит юзеру один раз за всю жизнь (реферальный анлок, миграция).
    """
    name: str
    kind: str                                   # 'marketing' | 'transactional'
    text: str | Callable[[dict], str]
    recipients: Callable[[], list[dict]]
    buttons: Callable[[dict], list[list[dict]]] | None = None
    once: bool = True
    meta: dict = field(default_factory=dict)

    def render(self, user: dict) -> str:
        return self.text(user) if callable(self.text) else self.text


def _keyboard(campaign: Campaign, user: dict) -> dict | None:
    rows = campaign.buttons(user) if campaign.buttons else []
    if campaign.kind == "marketing":
        # Выход дешевле блокировки — раздражённый юзер почти всегда выберет его.
        rows = rows + [[{"text": "🔕 Реже писать", "callback_data": OPT_OUT_CALLBACK}]]
    return {"inline_keyboard": rows} if rows else None


def eligible(campaign: Campaign, ignore_cap: bool = False) -> list[dict]:
    """Кому реально уйдёт: после отсева по блоку, opt-out, капу, идемпотентности.

    ignore_cap=True снимает ТОЛЬКО 30-дневный кап маркетинга — для разового бласта,
    который Артём утвердил осознанно. Идемпотентность (`once`) и opt-out не снимаются
    никогда. Через крон с этим флагом не ходить: кап и существует, чтобы автоматика
    не долбила людей.
    """
    out = []
    for u in campaign.recipients():
        tg_id = u["tg_id"]
        if campaign.once and db.was_notified(tg_id, campaign.name):
            continue
        if (not ignore_cap and campaign.kind == "marketing"
                and db.marketing_sent_since(tg_id, MARKETING_CAP_DAYS) > 0):
            continue
        out.append(u)
    return out


def preview(campaign: Campaign) -> str:
    """Что уйдёт и кому — для утверждения ДО отправки."""
    users = eligible(campaign)
    sample = campaign.render(users[0]) if users else campaign.render({"tg_id": 0})
    who = ", ".join(str(u["tg_id"]) for u in users[:10])
    tail = f" … и ещё {len(users) - 10}" if len(users) > 10 else ""
    return (
        f"Кампания: {campaign.name} ({campaign.kind})\n"
        f"Получателей: {len(users)}\n"
        f"Кому: {who or '—'}{tail}\n\n"
        f"Текст (как увидит юзер):\n{sample}"
    )


def _send_one(token: str, tg_id: int, text: str, kb: dict | None) -> tuple[str, float]:
    """→ ('sent'|'blocked'|'retry'|'failed', retry_after_sec)."""
    payload = {"chat_id": tg_id, "text": text, "parse_mode": "HTML"}
    if kb:
        payload["reply_markup"] = json.dumps(kb)
    r = requests.post(API.format(token=token), data=payload, timeout=20)
    if r.ok:
        return "sent", 0.0

    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    code = body.get("error_code", r.status_code)
    if code == 403:
        return "blocked", 0.0   # заблокировал бота — ретраить нечего, канал потерян
    if code == 429:
        return "retry", float(body.get("parameters", {}).get("retry_after", 5))
    logger.error(f"send to {tg_id} failed: {code} {body.get('description', r.text[:120])}")
    return "failed", 0.0


def send(token: str, campaign: Campaign, dry_run: bool = True,
         ignore_cap: bool = False) -> dict:
    """Отправка. dry_run=True (по умолчанию) не шлёт ничего — только считает."""
    users = eligible(campaign, ignore_cap=ignore_cap)
    stats = {"eligible": len(users), "sent": 0, "blocked": 0, "failed": 0, "dry_run": dry_run}
    if dry_run:
        return stats

    for u in users:
        tg_id = u["tg_id"]
        text = campaign.render(u)
        kb = _keyboard(campaign, u)

        for _ in range(MAX_RETRIES):
            status, retry_after = _send_one(token, tg_id, text, kb)
            if status == "retry":
                logger.warning(f"{campaign.name}: 429 for {tg_id}, sleep {retry_after}s")
                time.sleep(retry_after)
                continue
            if status == "sent":
                db.mark_bot_blocked(tg_id, False)  # дошло → снимаем прежний блок, если был
                db.log_notification(tg_id, campaign.name, campaign.kind, campaign.meta)
                stats["sent"] += 1
            elif status == "blocked":
                db.mark_bot_blocked(tg_id)
                stats["blocked"] += 1
            else:
                stats["failed"] += 1
            break
        else:
            stats["failed"] += 1

        time.sleep(1 / RATE_LIMIT_PER_SEC)

    return stats
