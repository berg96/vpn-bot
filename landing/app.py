import hashlib
import logging
import os
import secrets
import time
from datetime import datetime, timezone, timedelta

import aiohttp
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

import db
import marzban
import robokassa
import tinkoff
import sub_tokens

# Отдельный TG-client для уведомлений юзера после успешной RUB-оплаты.
# Т-Банк webhook прилетает на лендинг, а не на бота — нужен свой Bot instance.
# Тяжёлых операций с aiogram не делаем, только send_message.
from aiogram import Bot as _TGBot
_tg_bot: _TGBot | None = None
if os.environ.get("BOT_TOKEN"):
    _tg_bot = _TGBot(token=os.environ["BOT_TOKEN"])

# Отдельный бот поддержки для алертов в топик группы
_support_bot: _TGBot | None = None
if os.environ.get("SUPPORT_BOT_TOKEN"):
    _support_bot = _TGBot(token=os.environ["SUPPORT_BOT_TOKEN"])

SUPPORT_GROUP_ID = int(os.environ.get("SUPPORT_GROUP_ID", 0))
ALERT_TOPIC_ID = int(os.environ.get("ALERT_TOPIC_ID", 33))

# Ручной kill-switch: пока касса не работает, ставим в .env CARD_PAYMENT_ENABLED=0,
# чтобы /pay сразу показывал «недоступно» без обращения к API эквайринга.
CARD_PAYMENT_ENABLED = os.environ.get("CARD_PAYMENT_ENABLED", "1") == "1"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_USERNAME = os.environ.get("BOT_USERNAME", "radarshield_bot")
SUPPORT_USERNAME = os.environ.get("SUPPORT_USERNAME", "radarshield_support_bot")
IP_FULL_NAME = os.environ.get("IP_FULL_NAME", "ИП Куликов Артём Владиславович")
IP_INN = os.environ.get("IP_INN", "")
IP_OGRNIP = os.environ.get("IP_OGRNIP", "")
IP_ADDRESS = os.environ.get("IP_ADDRESS", "")
SUPPORT_EMAIL = os.environ.get("SUPPORT_EMAIL", "chigar2010@yandex.ru")
SUPPORT_PHONE = os.environ.get("SUPPORT_PHONE", "")
DOCS_UPDATED_AT = os.environ.get("DOCS_UPDATED_AT", "23 апреля 2026 г.")


def _page_context(request):
    """Общие переменные для всех рендеров — реквизиты ИП, контакты, футер."""
    return {
        "request": request,
        "bot_username": BOT_USERNAME,
        "support_username": SUPPORT_USERNAME,
        "ip_full_name": IP_FULL_NAME,
        "ip_inn": IP_INN,
        "ip_ogrnip": IP_OGRNIP,
        "ip_address": IP_ADDRESS,
        "support_email": SUPPORT_EMAIL,
        "support_phone": SUPPORT_PHONE,
        "updated_at": DOCS_UPDATED_AT,
    }


# алиас для шаблонов документов — они используют тот же контекст
_doc_context = _page_context

# --- Acquiring health check (кеш 5 минут) ---
_acquiring_health: tuple[bool, float] = (True, 0.0)
# Глобальный счётчик последовательных провалов (на реальных проверках, не закешированных)
_fail_streak: int = 0
_fail_alerted: bool = False  # алерт уже отправлен в текущей "полосе" ошибок


async def _send_acquiring_alert() -> None:
    bot = _support_bot or _tg_bot
    if not bot or not SUPPORT_GROUP_ID:
        return
    try:
        await bot.send_message(
            -1000000000000 - SUPPORT_GROUP_ID if SUPPORT_GROUP_ID > 0 else SUPPORT_GROUP_ID,
            "⚠️ <b>Эквайринг недоступен — 5 проверок подряд</b>\n\n"
            "Health check возвращает ошибку уже 5 раз подряд.\n"
            "Проверь настройки эквайринга / CARD_PAYMENT_ENABLED.",
            parse_mode="HTML",
            message_thread_id=ALERT_TOPIC_ID,
        )
    except Exception as e:
        logger.error(f"acquiring alert send failed: {e}")


async def _acquiring_healthy() -> bool:
    """Проверяет доступность эквайринга. Кеш 5 мин, алерт на 5 провалов подряд."""
    global _acquiring_health, _fail_streak, _fail_alerted
    now = time.time()
    if now - _acquiring_health[1] < 300:
        return _acquiring_health[0]
    try:
        async with aiohttp.ClientSession() as s:
            ok = await robokassa.health_check(s)
    except Exception as e:
        logger.error(f"Acquiring health check failed: {e}")
        ok = False
    _acquiring_health = (ok, now)
    if ok:
        _fail_streak = 0
        _fail_alerted = False
    else:
        _fail_streak += 1
        if _fail_streak >= 5 and not _fail_alerted:
            _fail_alerted = True
            await _send_acquiring_alert()
    return ok

IP_SALT = os.environ.get("LANDING_IP_SALT", "change-me")
TRIAL_HOURS = int(os.environ.get("LANDING_TRIAL_HOURS", "3"))
TRIAL_DATA_MB = int(os.environ.get("LANDING_TRIAL_MB", "500"))
IP_SOFT_LIMIT_24H = int(os.environ.get("LANDING_IP_SOFT_LIMIT", "5"))

app = FastAPI(title="RadarShield landing", docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory="landing/static"), name="static")
templates = Jinja2Templates(directory="landing/templates")

db.init_db()

APP_DOWNLOADS = {
    "flclash_android": "https://github.com/chen08209/FlClash/releases/download/v0.8.92/FlClash-0.8.92-android-arm64-v8a.apk",
    "flclash_windows": "https://github.com/chen08209/FlClash/releases/download/v0.8.92/FlClash-0.8.92-windows-amd64-setup.exe",
    "flclash_macos_arm": "https://github.com/chen08209/FlClash/releases/download/v0.8.92/FlClash-0.8.92-macos-arm64.dmg",
    "flclash_macos_x64": "https://github.com/chen08209/FlClash/releases/download/v0.8.92/FlClash-0.8.92-macos-amd64.dmg",
    "karing_ios": "https://apps.apple.com/app/karing/id6472431552",
}


def _client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for", "").split(",")[0].strip()
    return xff or (request.client.host if request.client else "0.0.0.0")


def _ip_hash(request: Request) -> str:
    return hashlib.sha256(f"{_client_ip(request)}|{IP_SALT}".encode()).hexdigest()


def _client_fp(request: Request) -> str:
    """Серверный fingerprint: IP + User-Agent + Accept-Language. Клиент очистить не может —
    единственный слой, который ловит Telegram in-app browser (cookie/localStorage он стирает).
    """
    ip = _client_ip(request)
    ua = request.headers.get("user-agent", "")
    al = request.headers.get("accept-language", "")
    return hashlib.sha256(f"{ip}|{ua}|{al}|{IP_SALT}".encode()).hexdigest()


def _is_lead_active(lead: dict) -> bool:
    return datetime.fromisoformat(lead["expires_at"]) > datetime.now(timezone.utc)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    existing_token = request.cookies.get("lp_token")
    active_token = None
    if existing_token:
        lead = db.get_landing_lead(existing_token)
        if lead and _is_lead_active(lead) and lead.get("claimed_tg_id") is None:
            active_token = existing_token
    return templates.TemplateResponse("index.html", {
        **_page_context(request),
        "downloads": APP_DOWNLOADS,
        "trial_hours": TRIAL_HOURS,
        "active_token": active_token,
    })


@app.post("/trial")
async def create_trial(
    request: Request,
    device_id: str = Form(...),
    fingerprint: str = Form(""),
):
    existing_token = request.cookies.get("lp_token")
    if existing_token:
        lead = db.get_landing_lead(existing_token)
        if lead and _is_lead_active(lead):
            return RedirectResponse(url=f"/s/{existing_token}", status_code=303)

    client_fp = _client_fp(request)

    def already_tried(reason: str, status: int = 403):
        return templates.TemplateResponse(
            "already-tried.html",
            {
                "request": request,
                "message": reason,
                "bot_username": BOT_USERNAME,
                "support_username": SUPPORT_USERNAME,
            },
            status_code=status,
        )

    # Сначала — reuse активного лида по серверному fingerprint (Telegram in-app browser).
    server_lead = db.find_lead_by_client_fp(client_fp)
    if server_lead:
        existing = db.get_landing_lead(server_lead["token"])
        if existing and _is_lead_active(existing):
            resp = RedirectResponse(url=f"/s/{existing['token']}", status_code=303)
            resp.set_cookie(
                "lp_token", existing["token"],
                max_age=TRIAL_HOURS * 3600,
                httponly=True, secure=True, samesite="lax",
            )
            return resp
        return already_tried(
            "С этого устройства уже был пробный период. Забери 7 дней в Telegram — через бота."
        )

    if db.find_lead_by_device(device_id):
        return already_tried(
            "На этом устройстве уже был пробный период. Забери 7 дней в Telegram — через бота."
        )

    if fingerprint and db.find_lead_by_fingerprint(fingerprint):
        return already_tried(
            "На этом устройстве уже был пробный период. Забери 7 дней в Telegram — через бота."
        )

    ip_h = _ip_hash(request)
    if db.count_ip_leads_24h(ip_h) >= IP_SOFT_LIMIT_24H:
        return already_tried(
            "Слишком много пробных периодов с этого IP за сутки. Попробуй позже или заходи в Telegram-бота.",
            status=429,
        )

    token = secrets.token_urlsafe(12)
    mz_name = f"lp_{token}"
    expires_at = (datetime.now(timezone.utc) + timedelta(hours=TRIAL_HOURS)).isoformat()

    try:
        async with aiohttp.ClientSession() as s:
            user_data = await marzban.create_landing_trial(
                s, mz_name, hours=TRIAL_HOURS, data_limit_mb=TRIAL_DATA_MB,
            )
    except Exception as e:
        logger.error(f"create_landing_trial failed: {e}")
        raise HTTPException(status_code=502, detail="Не удалось создать триал, попробуй чуть позже.")

    sub_url = user_data.get("subscription_url") or None

    db.create_landing_lead(
        token=token,
        mz_username=mz_name,
        device_id=device_id,
        fingerprint=fingerprint or None,
        ip_hash=ip_h,
        client_fp=client_fp,
        user_agent=request.headers.get("user-agent", "")[:500],
        expires_at=expires_at,
        sub_url=sub_url,
    )

    resp = RedirectResponse(url=f"/s/{token}", status_code=303)
    resp.set_cookie(
        "lp_token",
        token,
        max_age=TRIAL_HOURS * 3600,
        httponly=True,
        secure=True,
        samesite="lax",
    )
    return resp


def _is_unlimited(user: dict) -> bool:
    """Безлимит-бессрочно (data_limit=0/None И expire=0/None)."""
    return not (user.get("data_limit") or 0) and not (user.get("expire") or 0)


def _is_active(user: dict) -> bool:
    """Подписка активна: status=active И не истёк expire."""
    if (user.get("status") or "").lower() != "active":
        return False
    expire = user.get("expire") or 0
    if expire == 0:
        return True  # бессрочно
    return expire > int(datetime.now(timezone.utc).timestamp())


def _expired_sub_response():
    """Поддельный sub с одним фейк-сервером, имя = призыв продлить.

    В Karing/FLClash отображается как сервер с именем — пользователь
    видит ссылку на оплату прямо в списке серверов клиента.
    """
    import base64 as _b64
    from urllib.parse import quote
    from fastapi.responses import Response as _Resp

    remark = "🔴 Подписка кончилась — radarshield.mooo.com/pay"
    dummy = (
        f"vless://00000000-0000-0000-0000-000000000000@127.0.0.1:443"
        f"?type=tcp&security=none#{quote(remark)}"
    )
    body = _b64.b64encode(dummy.encode()).decode().encode()
    return _Resp(
        content=body,
        media_type="text/plain; charset=utf-8",
        headers={"content-disposition": 'attachment; filename="RadarShield VPN"'},
    )


async def _proxy_marzban_sub(sub_url: str, request: Request):
    """Скачивает sub-контент из Marzban (внутренний URL) и возвращает клиенту."""
    from fastapi.responses import Response as _Resp

    # Заменяем публичный домен на внутренний Marzban — иначе nginx зациклится
    # (nginx /sub/ → наш app → fetch public URL → nginx /sub/ → loop)
    _mz_base = os.environ.get("MARZBAN_URL", "http://127.0.0.1:8000")
    for _prefix in ("https://", "http://"):
        if sub_url.startswith(_prefix):
            _path = sub_url[len(_prefix):].split("/", 1)
            if len(_path) == 2:
                sub_url = f"{_mz_base}/{_path[1]}"
            break

    ua = request.headers.get("user-agent", "")
    try:
        async with aiohttp.ClientSession() as s:
            resp = await s.get(sub_url, headers={"User-Agent": ua}, timeout=aiohttp.ClientTimeout(total=15))
            content = await resp.read()
            content_type = resp.headers.get("Content-Type", "text/plain; charset=utf-8")
    except Exception as e:
        logger.error(f"proxy_sub fetch failed: {e}")
        raise HTTPException(status_code=502, detail="Upstream error")

    return _Resp(
        content=content,
        media_type=content_type,
        headers={"content-disposition": 'attachment; filename="RadarShield VPN"'},
    )


@app.get("/sub/{token}")
async def proxy_sub(token: str, request: Request):
    """Стабильный прокси подписки.

    Два пути:
    1. HMAC-токен (новый формат) — стабильная ссылка по tg_id, никогда не
       меняется. Подделка невозможна без SUB_TOKEN_SECRET. Если подписка
       истекла — возвращаем фейк-sub с призывом продлить.
    2. Marzban-base64 токен (legacy) — пускаем только бессрочных безлимитчиков
       (data_limit=0 AND expire=0): они уже вставили старые ссылки в клиенты,
       не хотим их тревожить просьбой переподключиться.
    """
    # --- Path 1: HMAC token ---
    tg_id = sub_tokens.parse_sub_token(token)
    if tg_id is not None:
        with db._conn() as c:
            row = c.execute(
                "SELECT mz_username FROM users WHERE tg_id=?", (tg_id,)
            ).fetchone()
        if not row or not row[0]:
            raise HTTPException(status_code=404, detail="Not found")
        mz_username = row[0]

        try:
            async with aiohttp.ClientSession() as s:
                user = await marzban.get_user(s, 0, mz_username=mz_username)
        except Exception as e:
            logger.error(f"proxy_sub marzban error for {mz_username}: {e}")
            raise HTTPException(status_code=502, detail="Upstream error")
        if not user or not user.get("subscription_url"):
            raise HTTPException(status_code=404, detail="Not found")

        if not _is_active(user):
            return _expired_sub_response()
        return await _proxy_marzban_sub(user["subscription_url"], request)

    # --- Path 2: legacy Marzban-base64 ---
    # Декодируем имя из марзбановского токена. Пускаем только если:
    #   (a) это landing-trial mz_user (имя `lp_XXX`, случайное и непредсказуемое), или
    #   (b) это tg-юзер на безлимите (data_limit=0 AND expire=0) — гранд-юзеры,
    #       которых не хотим тревожить просьбой переподключиться.
    # Для регулярных tg-юзеров с предсказуемым username (= tg_username) этот путь
    # закрыт — иначе подделка по имени тривиальна.
    import base64
    try:
        padding = (4 - len(token) % 4) % 4
        decoded = base64.b64decode(token + "=" * padding).decode("utf-8", errors="replace")
        mz_username = decoded.split(",")[0].strip()
        if not mz_username or "/" in mz_username:
            raise ValueError("bad username")
    except Exception:
        raise HTTPException(status_code=404, detail="Not found")

    try:
        async with aiohttp.ClientSession() as s:
            user = await marzban.get_user(s, 0, mz_username=mz_username)
    except Exception as e:
        logger.error(f"proxy_sub legacy marzban error for {mz_username}: {e}")
        raise HTTPException(status_code=502, detail="Upstream error")

    if not user or not user.get("subscription_url"):
        raise HTTPException(status_code=404, detail="Not found")

    is_landing_lead = mz_username.startswith("lp_")
    if not is_landing_lead and not _is_unlimited(user):
        raise HTTPException(status_code=404, detail="Not found")

    if not _is_active(user):
        return _expired_sub_response()
    return await _proxy_marzban_sub(user["subscription_url"], request)


@app.get("/s/{token}", response_class=HTMLResponse)
async def success(request: Request, token: str):
    lead = db.get_landing_lead(token)
    if not lead:
        raise HTTPException(status_code=404, detail="Пробный период не найден или срок жизни ссылки истёк.")

    # sub_url закеширован при создании lead'а — стабилен при refresh'ах.
    # Marzban отдаёт разные версии токена при каждом GET (timestamp-embed),
    # но все они эквивалентны. Показываем юзеру одну и ту же — UX чище.
    sub_url = lead.get("sub_url")
    if not sub_url:
        # Backfill для старых lead'ов, созданных до миграции колонки sub_url.
        try:
            async with aiohttp.ClientSession() as s:
                user = await marzban.get_user(s, 0, mz_username=lead["mz_username"])
            if user:
                sub_url = user.get("subscription_url")
                if sub_url:
                    db.set_landing_sub_url(lead["token"], sub_url)
        except Exception as e:
            logger.error(f"sub_url backfill failed for {lead['mz_username']}: {e}")

    expires_dt = datetime.fromisoformat(lead["expires_at"])
    active = _is_lead_active(lead)

    return templates.TemplateResponse("success.html", {
        **_page_context(request),
        "token": token,
        "sub_url": sub_url,
        "expires_at": expires_dt,
        "active": active,
        "claimed": lead["claimed_tg_id"] is not None,
        "downloads": APP_DOWNLOADS,
    })


@app.get("/i/{token}", response_class=HTMLResponse)
async def install(request: Request, token: str):
    """Deep-link redirector для landing-trial. Detect OS → URL-scheme клиента."""
    lead = db.get_landing_lead(token)
    if not lead or not lead.get("sub_url"):
        raise HTTPException(status_code=404, detail="Пробный период не найден или срок жизни ссылки истёк.")

    # Если lead уже claimed/merged — mz-юзер может быть удалён из Marzban,
    # старая sub_url отдаст битый конфиг. Отправляем на страницу успеха.
    if lead.get("claimed_tg_id") is not None:
        return RedirectResponse(url=f"/s/{token}", status_code=303)

    return templates.TemplateResponse("install.html", {
        "request": request,
        "sub_url": lead["sub_url"],
        "back_url": f"/s/{token}",
        "downloads": APP_DOWNLOADS,
        "bot_username": BOT_USERNAME,
        "support_username": SUPPORT_USERNAME,
    })


@app.get("/open/{mz_username}", response_class=HTMLResponse)
async def open_app(request: Request, mz_username: str):
    """Deep-link redirector для основной подписки.

    Для tg-юзеров (есть в users.mz_username) выдаём стабильную HMAC-ссылку
    `/sub/{token}` — она никогда не меняется. Для landing-leads (имя `lp_XXX`,
    нет tg_id) выдаём marzban-URL и кэшируем в БД, чтобы UI не «прыгал»
    при refresh'ах.
    """
    with db._conn() as c:
        row = c.execute(
            "SELECT tg_id FROM users WHERE mz_username=?", (mz_username,)
        ).fetchone()
    tg_id = row[0] if row else None

    if tg_id:
        sub_url = f"https://radarshield.mooo.com/sub/{sub_tokens.make_sub_token(tg_id)}"
    else:
        sub_url = db.get_sub_url_by_mz(mz_username)
        if not sub_url:
            try:
                async with aiohttp.ClientSession() as s:
                    user = await marzban.get_user(s, 0, mz_username=mz_username)
            except Exception as e:
                logger.error(f"open_app marzban fetch failed for {mz_username}: {e}")
                user = None
            if not user or not user.get("subscription_url"):
                raise HTTPException(status_code=404, detail="Подписка не найдена. Возможно, она была удалена.")
            sub_url = user["subscription_url"]
            db.set_sub_url_by_mz(mz_username, sub_url)

    return templates.TemplateResponse("install.html", {
        "request": request,
        "sub_url": sub_url,
        "back_url": None,
        "downloads": APP_DOWNLOADS,
        "bot_username": BOT_USERNAME,
        "support_username": SUPPORT_USERNAME,
    })


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Все 404 (и прочие не-хендленные HTTPException без красивой HTML)
    рендерятся через шаблон 404.html вместо сырого JSON."""
    if exc.status_code == 404:
        return templates.TemplateResponse(
            "404.html",
            {
                **_page_context(request),
                "heading": "🔎 Страница не найдена",
                "message": str(exc.detail) if exc.detail and exc.detail != "Not Found"
                          else "Ссылка устарела или такой страницы никогда не было.",
            },
            status_code=404,
        )
    # прочие ошибки — дефолтное поведение
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


def _verify_pay_sig(uid: str, sig: str) -> bool:
    import hmac, hashlib
    secret = os.environ.get("PAY_LINK_SECRET", os.environ.get("BOT_TOKEN", "")[:32])
    expected = hmac.new(secret.encode(), str(uid).encode(), hashlib.sha256).hexdigest()[:16]
    return hmac.compare_digest(sig, expected)


@app.get("/pay", response_class=HTMLResponse)
async def pay_page(request: Request, uid: str = "", sig: str = ""):
    if not uid or not sig or not _verify_pay_sig(uid, sig):
        return templates.TemplateResponse(
            "pay-result.html",
            {
                **_page_context(request),
                "title": "Оплата подписки",
                "heading": "💳 Оплата через бота",
                "message": "Оплата подписки доступна только по персональной ссылке, которую выдаёт Telegram-бот. Открой @"
                           + BOT_USERNAME + " и нажми «Оплатить» — будет выдана новая ссылка с актуальным тарифом.",
                "success": False,
            },
            status_code=200,
        )

    if not CARD_PAYMENT_ENABLED or not await _acquiring_healthy():
        return templates.TemplateResponse(
            "pay-result.html",
            {
                **_page_context(request),
                "title": "Оплата временно недоступна",
                "heading": "🛠 Оплата картой временно недоступна",
                "message": (
                    "К сожалению, оплата картой сейчас временно недоступна. "
                    "Попробуйте позже или обратитесь в службу поддержки — поможем оформить вручную."
                ),
                "success": False,
            },
            status_code=200,
        )

    import config as _cfg
    plans_display = {}
    for key, (name, days, stars, stars_str, rub_kopeks, rub_str) in _cfg.PLANS.items():
        per_month = (rub_kopeks / 100) / (days / 30)
        plans_display[key] = {
            "name": name,
            "rub_str": rub_str,
            "rub_kopeks": rub_kopeks,
            "per_month": int(per_month) if days != 30 else None,
            "discount": int((1 - per_month / 250) * 100) if days != 30 else None,
        }
    return templates.TemplateResponse("pay.html", {
        **_page_context(request),
        "uid": uid,
        "sig": sig,
        "plans": plans_display,
    })


@app.post("/api/tinkoff/init")
async def tinkoff_init_payment(request: Request):
    data = await request.json()
    uid = str(data.get("uid", ""))
    sig = data.get("sig", "")
    plan_key = data.get("plan", "")

    if not uid or not sig or not _verify_pay_sig(uid, sig):
        return {"error": "Ссылка недействительна"}

    import config as _cfg
    plan = _cfg.PLANS.get(plan_key)
    if not plan:
        return {"error": "Неизвестный тариф"}
    name, days, stars, stars_str, rub_kopeks, rub_str = plan

    tg_id = int(uid)
    order_id = f"rs-{tg_id}-{plan_key}-{int(datetime.now(timezone.utc).timestamp())}"

    try:
        async with aiohttp.ClientSession() as s:
            result = await tinkoff.init_payment(
                s,
                amount_kopeks=rub_kopeks,
                order_id=order_id,
                description=f"{tinkoff.RECEIPT_ITEM_NAME_PREFIX}, {name}",
                customer_key=uid,
            )
    except Exception as e:
        logger.error(f"tinkoff init failed from /pay: {e}")
        return {"error": "Не удалось создать счёт, попробуй ещё раз"}

    db.record_tinkoff_pending(
        order_id=order_id, tg_id=tg_id, plan_key=plan_key,
        amount_kopeks=rub_kopeks, payment_id=str(result.get("PaymentId", "")),
    )
    db.log_event(tg_id, "pay_initiated",
                 {"plan": plan_key, "amount_rub": rub_kopeks / 100, "provider": "tinkoff"})

    return {"payment_url": result.get("PaymentURL"), "order_id": order_id}


@app.post("/api/tinkoff/notification")
async def tinkoff_notification(request: Request):
    """Webhook от Т-Банка. Приходит при каждом изменении статуса платежа.

    Делаем:
      1. Верифицируем Token (защита от подделки).
      2. Смотрим OrderId в БД — наш ли это платёж.
      3. Если Status=CONFIRMED и ещё не обработан — активируем подписку в Marzban,
         шлём юзеру уведомление в TG.
      4. Возвращаем "OK" — Т-Банк ждёт именно эту строку, иначе будет ретраить.
    """
    try:
        payload = await request.json()
    except Exception:
        logger.error("tinkoff notification: invalid JSON")
        return "OK"

    logger.info(f"tinkoff notification: {payload}")

    if not tinkoff.verify_notification(payload):
        logger.error(f"tinkoff notification: bad token, payload={payload}")
        return "OK"  # Всё равно OK — иначе Т-Банк зациклится в ретраях

    order_id = payload.get("OrderId")
    status = payload.get("Status", "")
    if not order_id:
        return "OK"

    payment = db.get_tinkoff_payment(order_id)
    if not payment:
        logger.warning(f"tinkoff notification: unknown order_id={order_id}")
        return "OK"

    if status == "CONFIRMED":
        # Атомарно пытаемся пометить как confirmed — если уже было, выходим (идемпотентность).
        if not db.mark_tinkoff_confirmed(order_id):
            return "OK"

        tg_id = payment["tg_id"]
        plan_key = payment["plan_key"]

        import config
        plan = config.PLANS.get(plan_key)
        if not plan:
            logger.error(f"tinkoff confirm: unknown plan {plan_key}")
            return "OK"
        name, days, stars, stars_str, rub_kopeks, rub_str = plan

        # Активируем подписку — та же логика что в bot.successful_payment
        db.record_payment(tg_id=tg_id, plan_key=plan_key,
                          stars=rub_kopeks, charge_id=str(payload.get("PaymentId", "")))
        db.delete_reminder_events(tg_id)  # продлили → reset напоминаний
        db.log_event(tg_id, "pay_success",
                     {"plan": plan_key, "amount_rub": rub_kopeks / 100, "provider": "tinkoff"})

        try:
            async with aiohttp.ClientSession() as s:
                mz_username = db.get_mz_username(tg_id)
                if not mz_username:
                    # edge-case: нет связанного mz_user. Создадим по tg_username-паттерну.
                    mz_username = marzban.build_mz_username(tg_id)
                    db.set_mz_username(tg_id, mz_username)
                user_data = await marzban.create_or_extend_user(s, tg_id, days, mz_username=mz_username)
            mz_sub_url = user_data.get("subscription_url", "")
            if mz_sub_url:
                db.set_sub_url(tg_id, mz_sub_url)
            sub_url = f"https://radarshield.mooo.com/sub/{sub_tokens.make_sub_token(tg_id)}"
            expire_ts = user_data.get("expire")
            expire_str = (
                datetime.fromtimestamp(expire_ts, tz=timezone.utc).strftime("%d.%m.%Y")
                if expire_ts else "—"
            )
        except Exception as e:
            logger.error(f"tinkoff confirm: marzban failed for {order_id}: {e}")
            # Не теряем платёж: юзер заплатил, активируем в следующий раз вручную
            return "OK"

        # Пишем юзеру в Telegram
        if _tg_bot and tg_id:
            from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
            try:
                await _tg_bot.send_message(
                    tg_id,
                    f"✅ <b>Оплата {rub_str} прошла! Подписка активирована до {expire_str}</b>\n\n"
                    f"🔗 Твоя ссылка:\n<code>{sub_url}</code>\n\n"
                    "Нажми <b>📲 Открыть в приложении</b> — клиент откроется с готовым импортом.",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
                        [InlineKeyboardButton(text="📲 Открыть в приложении",
                                              url=f"https://radarshield.mooo.com/open/{mz_username}")],
                    ]),
                )
                if int(os.environ.get("ADMIN_TG_ID", 0)):
                    await _tg_bot.send_message(
                        int(os.environ["ADMIN_TG_ID"]),
                        f"💰 RUB-оплата: tg {tg_id}, план {plan_key}, {rub_str}",
                    )
            except Exception as e:
                logger.error(f"tinkoff confirm: tg notify failed: {e}")

    elif status == "REJECTED":
        db.mark_tinkoff_rejected(order_id)
        db.log_event(payment["tg_id"], "pay_rejected",
                     {"plan": payment["plan_key"], "provider": "tinkoff"})

    return "OK"


@app.post("/api/robokassa/init")
async def robokassa_init_payment(request: Request):
    data = await request.json()
    uid = str(data.get("uid", ""))
    sig = data.get("sig", "")
    plan_key = data.get("plan", "")

    if not uid or not sig or not _verify_pay_sig(uid, sig):
        return {"error": "Ссылка недействительна"}

    import config as _cfg
    plan = _cfg.PLANS.get(plan_key)
    if not plan:
        return {"error": "Неизвестный тариф"}
    name, days, stars, stars_str, rub_kopeks, rub_str = plan

    tg_id = int(uid)
    amount_rub = rub_kopeks / 100

    inv_id = db.create_robokassa_pending(
        tg_id=tg_id, plan_key=plan_key, amount_kopeks=rub_kopeks,
    )

    item_name = f"{robokassa.RECEIPT_ITEM_NAME_PREFIX}, {name}"
    receipt = robokassa.build_receipt(item_name=item_name, amount_rub=amount_rub)
    payment_url = robokassa.make_payment_url(
        inv_id=inv_id,
        amount_rub=amount_rub,
        description=item_name,
        receipt=receipt,
        customer_email=robokassa.RECEIPT_DEFAULT_EMAIL,
        shp={"Shp_plan": plan_key, "Shp_uid": str(tg_id)},
    )

    db.log_event(tg_id, "pay_initiated",
                 {"plan": plan_key, "amount_rub": amount_rub,
                  "provider": "robokassa", "inv_id": inv_id})

    return {"payment_url": payment_url, "inv_id": inv_id}


@app.post("/api/robokassa/result")
async def robokassa_result(request: Request):
    """Webhook от Robokassa. Шлётся после авторизации платежа.
    Должны вернуть 'OK{InvId}' plain-text — иначе ретраи."""
    form = await request.form()
    payload = {k: str(v) for k, v in form.items()}
    logger.info(f"robokassa result: {payload}")

    out_sum = payload.get("OutSum", "")
    inv_id_str = payload.get("InvId", "")
    signature = payload.get("SignatureValue", "")
    shp = {k: v for k, v in payload.items() if k.startswith("Shp_")}

    if not robokassa.verify_result(out_sum, inv_id_str, signature, shp):
        logger.error(f"robokassa result: bad signature, payload={payload}")
        return PlainTextResponse("bad signature", status_code=400)

    try:
        inv_id = int(inv_id_str)
    except ValueError:
        return PlainTextResponse("bad inv_id", status_code=400)

    payment = db.get_robokassa_payment(inv_id)
    if not payment:
        logger.warning(f"robokassa result: unknown inv_id={inv_id}")
        return PlainTextResponse(f"OK{inv_id}", status_code=200)

    # Идемпотентность: дубль webhook'а просто отвечает OK без повторной активации.
    if not db.mark_robokassa_confirmed(inv_id):
        return PlainTextResponse(f"OK{inv_id}", status_code=200)

    tg_id = payment["tg_id"]
    plan_key = payment["plan_key"]
    rub_kopeks = payment["amount_kopeks"]

    import config
    plan = config.PLANS.get(plan_key)
    if not plan:
        logger.error(f"robokassa confirm: unknown plan {plan_key}")
        return PlainTextResponse(f"OK{inv_id}", status_code=200)
    name, days, stars, stars_str, _rub_kopeks, rub_str = plan

    db.record_payment(tg_id=tg_id, plan_key=plan_key,
                      stars=rub_kopeks, charge_id=f"robokassa-{inv_id}")
    db.delete_reminder_events(tg_id)
    db.log_event(tg_id, "pay_success",
                 {"plan": plan_key, "amount_rub": rub_kopeks / 100,
                  "provider": "robokassa", "inv_id": inv_id})

    try:
        async with aiohttp.ClientSession() as s:
            mz_username = db.get_mz_username(tg_id)
            if not mz_username:
                mz_username = marzban.build_mz_username(tg_id)
                db.set_mz_username(tg_id, mz_username)
            user_data = await marzban.create_or_extend_user(s, tg_id, days, mz_username=mz_username)
        mz_sub_url = user_data.get("subscription_url", "")
        if mz_sub_url:
            db.set_sub_url(tg_id, mz_sub_url)
        sub_url = f"https://radarshield.mooo.com/sub/{sub_tokens.make_sub_token(tg_id)}"
        expire_ts = user_data.get("expire")
        expire_str = (
            datetime.fromtimestamp(expire_ts, tz=timezone.utc).strftime("%d.%m.%Y")
            if expire_ts else "—"
        )
    except Exception as e:
        logger.error(f"robokassa confirm: marzban failed for inv_id={inv_id}: {e}")
        return PlainTextResponse(f"OK{inv_id}", status_code=200)

    if _tg_bot and tg_id:
        from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
        try:
            await _tg_bot.send_message(
                tg_id,
                f"✅ <b>Оплата {rub_str} прошла! Подписка активирована до {expire_str}</b>\n\n"
                f"🔗 Твоя ссылка:\n<code>{sub_url}</code>\n\n"
                "Нажми <b>📲 Открыть в приложении</b> — клиент откроется с готовым импортом.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="👤 Профиль", callback_data="profile")],
                    [InlineKeyboardButton(text="📲 Открыть в приложении",
                                          url=f"https://radarshield.mooo.com/open/{mz_username}")],
                ]),
            )
            if int(os.environ.get("ADMIN_TG_ID", 0)):
                await _tg_bot.send_message(
                    int(os.environ["ADMIN_TG_ID"]),
                    f"💰 Robokassa-оплата: tg {tg_id}, план {plan_key}, {rub_str}",
                )
        except Exception as e:
            logger.error(f"robokassa confirm: tg notify failed: {e}")

    return PlainTextResponse(f"OK{inv_id}", status_code=200)


@app.get("/pay/success", response_class=HTMLResponse)
async def pay_success(request: Request):
    return templates.TemplateResponse("pay-result.html", {
        "request": request,
        "success": True,
        "heading": "✅ Оплата прошла",
        "message": "Подписка активирована. Возвращайся в бота — там ссылка для подключения и все инструкции.",
        "title": "Оплата прошла",
        "bot_username": BOT_USERNAME,
        "support_username": SUPPORT_USERNAME,
    })


@app.get("/pay/fail", response_class=HTMLResponse)
async def pay_fail(request: Request):
    return templates.TemplateResponse("pay-result.html", {
        "request": request,
        "success": False,
        "heading": "❌ Оплата не прошла",
        "message": "Карта отклонена или что-то пошло не так. Попробуй ещё раз через бота или напиши в поддержку.",
        "title": "Оплата не прошла",
        "bot_username": BOT_USERNAME,
        "support_username": SUPPORT_USERNAME,
    })


@app.get("/offer", response_class=HTMLResponse)
async def doc_offer(request: Request):
    return templates.TemplateResponse("offer.html", {
        **_doc_context(request),
        "title": "Публичная оферта",
        "heading": "Публичная оферта (договор)",
    })


@app.get("/privacy", response_class=HTMLResponse)
async def doc_privacy(request: Request):
    return templates.TemplateResponse("privacy.html", {
        **_doc_context(request),
        "title": "Политика конфиденциальности",
        "heading": "Политика конфиденциальности",
    })


@app.get("/refund", response_class=HTMLResponse)
async def doc_refund(request: Request):
    return templates.TemplateResponse("refund.html", {
        **_doc_context(request),
        "title": "Условия возврата",
        "heading": "Условия возврата и отмены",
    })


@app.get("/security", response_class=HTMLResponse)
async def doc_security(request: Request):
    return templates.TemplateResponse("security.html", {
        **_doc_context(request),
        "title": "Безопасность данных",
        "heading": "Политика информационной безопасности",
    })


@app.get("/robots.txt", include_in_schema=False)
async def robots_txt():
    from fastapi.responses import FileResponse
    return FileResponse("landing/static/robots.txt", media_type="text/plain")


@app.get("/sitemap.xml", include_in_schema=False)
async def sitemap_xml():
    from fastapi.responses import FileResponse
    return FileResponse("landing/static/sitemap.xml", media_type="application/xml")


@app.get("/healthz")
async def healthz():
    return {"ok": True}
