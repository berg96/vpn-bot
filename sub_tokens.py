"""HMAC-signed subscription tokens.

Генерация и проверка стабильных токенов формата `base64url(tg_id || sig16)`,
где sig16 = HMAC-SHA256(SUB_TOKEN_SECRET, tg_id_bytes)[:16].

Свойство: для одного и того же tg_id токен **всегда один и тот же**, пока
жив SUB_TOKEN_SECRET. Подделать без секрета невозможно (2^128).

Пользователь вставляет ссылку `radarshield.mooo.com/sub/<token>` один раз
и навсегда — продление, смена лимита, ротация конфигов клиенту не видны.
"""

import base64
import hmac
import hashlib
import os

_SECRET = os.environ["SUB_TOKEN_SECRET"].encode()
_TG_ID_BYTES = 5  # 40 bits hold ~1.1T — Telegram IDs comfortably fit
_SIG_BYTES = 16   # 128 bits

#: Имя, под которым подписка ложится в клиент.
#:
#: Karing берёт имя профиля ИЗ САМОЙ ССЫЛКИ: фрагмент `#…` → `?remarks=` →
#: `?name=` → HTML-title → иначе домен (add_profile_by_link_or_content_screen.dart).
#: Заголовок `content-disposition` он игнорирует — поэтому профиль назывался
#: «radarshield.mooo.com». Фрагмент на сервер не отправляется, так что для FlClash
#: (он берёт имя из `content-disposition`) и для остальных клиентов это no-op.
PROFILE_NAME = "RadarShield"


def sub_url(tg_id: int, base: str = "https://radarshield.mooo.com") -> str:
    """Ссылка подписки для пользователя — с именем профиля во фрагменте."""
    return f"{base}/sub/{make_sub_token(tg_id)}#{PROFILE_NAME}"


def make_pay_sig(uid) -> str:
    """Подпись персональной ссылки (`?uid=&sig=`) — защита от подстановки чужого tg_id.

    Секрет отдельный от SUB_TOKEN_SECRET: ссылка живёт в письмах и переписке,
    компрометация не должна давать доступ к подписке.
    """
    secret = os.environ.get("PAY_LINK_SECRET", os.environ.get("BOT_TOKEN", "")[:32])
    return hmac.new(secret.encode(), str(uid).encode(), hashlib.sha256).hexdigest()[:16]


def verify_pay_sig(uid, sig: str) -> bool:
    return hmac.compare_digest(str(sig), make_pay_sig(uid))


def make_sub_token(tg_id: int) -> str:
    """Стабильный токен для пользователя tg_id."""
    if tg_id < 0 or tg_id >= 1 << (_TG_ID_BYTES * 8):
        raise ValueError(f"tg_id {tg_id} out of range")
    raw = tg_id.to_bytes(_TG_ID_BYTES, "big")
    sig = hmac.new(_SECRET, raw, hashlib.sha256).digest()[:_SIG_BYTES]
    return base64.urlsafe_b64encode(raw + sig).rstrip(b"=").decode()


def parse_sub_token(token: str) -> int | None:
    """Возвращает tg_id, если токен валиден, иначе None."""
    try:
        padding = (4 - len(token) % 4) % 4
        raw = base64.urlsafe_b64decode(token + "=" * padding)
    except Exception:
        return None
    if len(raw) != _TG_ID_BYTES + _SIG_BYTES:
        return None
    tg_id_bytes = raw[:_TG_ID_BYTES]
    sig = raw[_TG_ID_BYTES:]
    expected = hmac.new(_SECRET, tg_id_bytes, hashlib.sha256).digest()[:_SIG_BYTES]
    if not hmac.compare_digest(sig, expected):
        return None
    return int.from_bytes(tg_id_bytes, "big")
