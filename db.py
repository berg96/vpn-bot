import sqlite3
import os
import json
from datetime import datetime, timezone, timedelta

DB_PATH = os.environ.get("DB_PATH", "/data/vpn_bot.db")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                plan_key TEXT NOT NULL,
                stars INTEGER NOT NULL,
                telegram_charge_id TEXT,
                created_at TEXT NOT NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                tg_id INTEGER PRIMARY KEY,
                username TEXT,
                mz_username TEXT,
                first_seen TEXT NOT NULL,
                trial_activated INTEGER NOT NULL DEFAULT 0
            )
        """)
        # Миграции для существующих БД (idempotent ALTER TABLE)
        for col in ["mz_username TEXT", "sub_url TEXT", "last_seen TEXT"]:
            try:
                c.execute(f"ALTER TABLE users ADD COLUMN {col}")
            except Exception:
                pass
        # Events table for funnel/retention analytics.
        c.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                ts TEXT NOT NULL,
                meta TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS ix_events_name_ts ON events(name, ts)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_events_tg_id ON events(tg_id)")
        c.execute("""
            CREATE TABLE IF NOT EXISTS landing_leads (
                token TEXT PRIMARY KEY,
                mz_username TEXT NOT NULL UNIQUE,
                device_id TEXT NOT NULL,
                fingerprint TEXT,
                ip_hash TEXT NOT NULL,
                user_agent TEXT,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                claimed_tg_id INTEGER,
                claimed_at TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS ix_leads_device ON landing_leads(device_id)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_leads_fp ON landing_leads(fingerprint)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_leads_ip ON landing_leads(ip_hash, created_at)")
        # Миграция: серверный fingerprint (IP+UA+Accept-Language) — ловит TG in-app browser,
        # который стирает cookie/localStorage между сессиями.
        try:
            c.execute("ALTER TABLE landing_leads ADD COLUMN client_fp TEXT")
        except Exception:
            pass
        c.execute("CREATE INDEX IF NOT EXISTS ix_leads_client_fp ON landing_leads(client_fp)")
        # Кешируем sub_url чтобы не дёргать Marzban на каждый GET /s/{token}
        # и не показывать юзеру разные версии (Marzban генерит новый timestamp-embed при каждом запросе).
        try:
            c.execute("ALTER TABLE landing_leads ADD COLUMN sub_url TEXT")
        except Exception:
            pass
        # Tinkoff rub-платежи: pending → confirmed/rejected. Webhook из Т-Банка сверяется с order_id.
        c.execute("""
            CREATE TABLE IF NOT EXISTS tinkoff_payments (
                order_id TEXT PRIMARY KEY,
                tg_id INTEGER NOT NULL,
                plan_key TEXT NOT NULL,
                amount_kopeks INTEGER NOT NULL,
                payment_id TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                confirmed_at TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS ix_tinkoff_payment_id ON tinkoff_payments(payment_id)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_tinkoff_tg_id ON tinkoff_payments(tg_id)")

        # Robokassa rub-платежи: pending → confirmed/rejected. InvId = autoincrement,
        # отдаётся юзеру в URL формы оплаты; Robokassa возвращает его в Result/Success.
        c.execute("""
            CREATE TABLE IF NOT EXISTS robokassa_payments (
                inv_id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                plan_key TEXT NOT NULL,
                amount_kopeks INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                confirmed_at TEXT,
                activated_at TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS ix_robokassa_tg_id ON robokassa_payments(tg_id)")
        # Миграция: добавить activated_at в существующие БД, где колонки ещё нет.
        cols = {r[1] for r in c.execute("PRAGMA table_info(robokassa_payments)").fetchall()}
        if "activated_at" not in cols:
            c.execute("ALTER TABLE robokassa_payments ADD COLUMN activated_at TEXT")
            # Старые confirmed помечаем активированными (они активировались inline в webhook).
            c.execute(
                "UPDATE robokassa_payments SET activated_at=confirmed_at "
                "WHERE status='confirmed' AND activated_at IS NULL"
            )


def record_user(tg_id: int, username: str | None) -> bool:
    """Возвращает True если пользователь новый."""
    with _conn() as c:
        cur = c.execute(
            "INSERT OR IGNORE INTO users (tg_id, username, first_seen) VALUES (?, ?, ?)",
            (tg_id, username, datetime.now(timezone.utc).isoformat()),
        )
        return cur.rowcount > 0


def get_mz_username(tg_id: int) -> str | None:
    with _conn() as c:
        row = c.execute(
            "SELECT mz_username FROM users WHERE tg_id=?", (tg_id,)
        ).fetchone()
        return row[0] if row else None


def find_user_by_username(username: str) -> dict | None:
    """Поиск юзера по Telegram username (case-insensitive, без @).
    Используется на лендинге для оплаты без захода в бота."""
    if not username:
        return None
    with _conn() as c:
        row = c.execute(
            "SELECT tg_id, username FROM users WHERE LOWER(username)=LOWER(?)",
            (username.lstrip("@"),),
        ).fetchone()
    return {"tg_id": row[0], "username": row[1]} if row else None


def get_username_by_tg_id(tg_id: int) -> str | None:
    with _conn() as c:
        row = c.execute(
            "SELECT username FROM users WHERE tg_id=?", (tg_id,)
        ).fetchone()
        return row[0] if row else None


def set_mz_username(tg_id: int, mz_username: str):
    with _conn() as c:
        c.execute(
            "UPDATE users SET mz_username=? WHERE tg_id=?", (mz_username, tg_id)
        )


def get_sub_url(tg_id: int) -> str | None:
    with _conn() as c:
        row = c.execute("SELECT sub_url FROM users WHERE tg_id=?", (tg_id,)).fetchone()
        return row[0] if row else None


def set_sub_url(tg_id: int, sub_url: str):
    with _conn() as c:
        c.execute("UPDATE users SET sub_url=? WHERE tg_id=?", (sub_url, tg_id))


def get_sub_url_by_mz(mz_username: str) -> str | None:
    """Единый кеш sub_url по mz_username — смотрит сначала users, потом landing_leads.
    Гарантирует что везде (бот /profile, /open/, /s/<token>) пользователь видит
    одну и ту же ссылку для одного и того же mz-юзера.
    Marzban каждый GET отдаёт новый timestamp в токене, БД-кэш стабилизирует вывод."""
    with _conn() as c:
        row = c.execute(
            "SELECT sub_url FROM users WHERE mz_username=? AND sub_url IS NOT NULL",
            (mz_username,),
        ).fetchone()
        if row and row[0]:
            return row[0]
        row = c.execute(
            "SELECT sub_url FROM landing_leads WHERE mz_username=? AND sub_url IS NOT NULL",
            (mz_username,),
        ).fetchone()
        return row[0] if row and row[0] else None


def set_sub_url_by_mz(mz_username: str, sub_url: str):
    """Сохраняет sub_url туда, где живёт этот mz — в users или landing_leads."""
    with _conn() as c:
        cur = c.execute(
            "UPDATE users SET sub_url=? WHERE mz_username=?", (sub_url, mz_username)
        )
        if cur.rowcount == 0:
            c.execute(
                "UPDATE landing_leads SET sub_url=? WHERE mz_username=?",
                (sub_url, mz_username),
            )


def is_trial_used(tg_id: int) -> bool:
    with _conn() as c:
        row = c.execute(
            "SELECT trial_activated FROM users WHERE tg_id=?", (tg_id,)
        ).fetchone()
        return bool(row and row[0])


def mark_trial_used(tg_id: int):
    with _conn() as c:
        c.execute(
            "UPDATE users SET trial_activated=1 WHERE tg_id=?", (tg_id,)
        )


def user_exists(tg_id: int) -> bool:
    with _conn() as c:
        row = c.execute("SELECT 1 FROM users WHERE tg_id=?", (tg_id,)).fetchone()
        return row is not None


def delete_user(tg_id: int):
    with _conn() as c:
        c.execute("DELETE FROM users WHERE tg_id=?", (tg_id,))


def record_payment(tg_id: int, plan_key: str, stars: int, charge_id: str):
    with _conn() as c:
        c.execute(
            "INSERT INTO payments (tg_id, plan_key, stars, telegram_charge_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (tg_id, plan_key, stars, charge_id, datetime.now(timezone.utc).isoformat()),
        )


def get_user_payments(tg_id: int) -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT plan_key, stars, created_at FROM payments WHERE tg_id=? ORDER BY created_at DESC",
            (tg_id,),
        ).fetchall()
    return [{"plan": r[0], "stars": r[1], "at": r[2]} for r in rows]


# ── Activity & events ────────────────────────────────────────────────────────

def touch_user(tg_id: int) -> None:
    """Update last_seen on every interaction. Called from middleware — must be cheap."""
    with _conn() as c:
        c.execute("UPDATE users SET last_seen=? WHERE tg_id=?", (_now_iso(), tg_id))


def log_event(tg_id: int, name: str, meta: dict | None = None) -> None:
    with _conn() as c:
        c.execute(
            "INSERT INTO events (tg_id, name, ts, meta) VALUES (?, ?, ?, ?)",
            (tg_id, name, _now_iso(), json.dumps(meta, ensure_ascii=False) if meta else None),
        )


# ── Stats helpers ────────────────────────────────────────────────────────────

def _iso_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def count_total_users() -> int:
    with _conn() as c:
        return c.execute("SELECT count(*) FROM users").fetchone()[0]


def count_new_users(days: int) -> int:
    with _conn() as c:
        return c.execute(
            "SELECT count(*) FROM users WHERE first_seen >= ?", (_iso_ago(days),)
        ).fetchone()[0]


def count_active_users(days: int) -> int:
    """Users with any interaction in the last N days (via last_seen)."""
    with _conn() as c:
        return c.execute(
            "SELECT count(*) FROM users WHERE last_seen >= ?", (_iso_ago(days),)
        ).fetchone()[0]


def count_trial_users() -> int:
    with _conn() as c:
        return c.execute("SELECT count(*) FROM users WHERE trial_activated = 1").fetchone()[0]


def count_paying_users() -> int:
    """Distinct users who made at least one payment."""
    with _conn() as c:
        return c.execute("SELECT count(DISTINCT tg_id) FROM payments").fetchone()[0]


def revenue_stars(days: int | None = None) -> int:
    with _conn() as c:
        if days is None:
            row = c.execute("SELECT COALESCE(SUM(stars), 0) FROM payments").fetchone()
        else:
            row = c.execute(
                "SELECT COALESCE(SUM(stars), 0) FROM payments WHERE created_at >= ?",
                (_iso_ago(days),),
            ).fetchone()
        return int(row[0])


def payment_count(days: int | None = None) -> int:
    with _conn() as c:
        if days is None:
            row = c.execute("SELECT count(*) FROM payments").fetchone()
        else:
            row = c.execute(
                "SELECT count(*) FROM payments WHERE created_at >= ?",
                (_iso_ago(days),),
            ).fetchone()
        return int(row[0])


def plan_distribution(days: int | None = None) -> list[tuple[str, int, int]]:
    """Returns list of (plan_key, count, total_stars) sorted by count desc."""
    with _conn() as c:
        if days is None:
            rows = c.execute(
                "SELECT plan_key, count(*), COALESCE(SUM(stars), 0) FROM payments "
                "GROUP BY plan_key ORDER BY count(*) DESC"
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT plan_key, count(*), COALESCE(SUM(stars), 0) FROM payments "
                "WHERE created_at >= ? GROUP BY plan_key ORDER BY count(*) DESC",
                (_iso_ago(days),),
            ).fetchall()
    return [(r[0], int(r[1]), int(r[2])) for r in rows]


def trial_to_paid_conversion() -> tuple[int, int]:
    """Returns (users_with_trial, users_with_trial_AND_payment)."""
    with _conn() as c:
        trial = c.execute(
            "SELECT count(*) FROM users WHERE trial_activated = 1"
        ).fetchone()[0]
        converted = c.execute(
            "SELECT count(DISTINCT u.tg_id) FROM users u "
            "JOIN payments p ON p.tg_id = u.tg_id "
            "WHERE u.trial_activated = 1"
        ).fetchone()[0]
    return int(trial), int(converted)


def retention_cohort(age_days: int, window_days: int = 1) -> tuple[int, int]:
    """Of users registered ~age_days ago, how many were seen in last `window_days`?

    Returns (cohort_size, still_active).
    """
    lower = (datetime.now(timezone.utc) - timedelta(days=age_days + 1)).isoformat()
    upper = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    active_since = _iso_ago(window_days)
    with _conn() as c:
        rows = c.execute(
            "SELECT count(*), "
            "COALESCE(SUM(CASE WHEN last_seen >= ? THEN 1 ELSE 0 END), 0) "
            "FROM users WHERE first_seen >= ? AND first_seen < ?",
            (active_since, lower, upper),
        ).fetchone()
    return int(rows[0]), int(rows[1])


# ── Landing leads ────────────────────────────────────────────────────────────

def create_landing_lead(
    token: str,
    mz_username: str,
    device_id: str,
    fingerprint: str | None,
    ip_hash: str,
    client_fp: str,
    user_agent: str | None,
    expires_at: str,
    sub_url: str | None,
) -> None:
    with _conn() as c:
        c.execute(
            "INSERT INTO landing_leads "
            "(token, mz_username, device_id, fingerprint, ip_hash, client_fp, user_agent, created_at, expires_at, sub_url) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (token, mz_username, device_id, fingerprint, ip_hash, client_fp, user_agent, _now_iso(), expires_at, sub_url),
        )


def get_landing_lead(token: str) -> dict | None:
    with _conn() as c:
        row = c.execute(
            "SELECT token, mz_username, device_id, fingerprint, ip_hash, "
            "created_at, expires_at, claimed_tg_id, claimed_at, sub_url "
            "FROM landing_leads WHERE token=?",
            (token,),
        ).fetchone()
    if not row:
        return None
    return {
        "token": row[0],
        "mz_username": row[1],
        "device_id": row[2],
        "fingerprint": row[3],
        "ip_hash": row[4],
        "created_at": row[5],
        "expires_at": row[6],
        "claimed_tg_id": row[7],
        "claimed_at": row[8],
        "sub_url": row[9],
    }


def find_lead_by_device(device_id: str) -> dict | None:
    """Любой lead (activo/claimed/expired) по device_id — для rate-limit за всё время."""
    with _conn() as c:
        row = c.execute(
            "SELECT token, claimed_tg_id, created_at FROM landing_leads "
            "WHERE device_id=? ORDER BY created_at DESC LIMIT 1",
            (device_id,),
        ).fetchone()
    if not row:
        return None
    return {"token": row[0], "claimed_tg_id": row[1], "created_at": row[2]}


def find_lead_by_fingerprint(fingerprint: str) -> dict | None:
    with _conn() as c:
        row = c.execute(
            "SELECT token, claimed_tg_id, created_at FROM landing_leads "
            "WHERE fingerprint=? ORDER BY created_at DESC LIMIT 1",
            (fingerprint,),
        ).fetchone()
    if not row:
        return None
    return {"token": row[0], "claimed_tg_id": row[1], "created_at": row[2]}


def find_lead_by_client_fp(client_fp: str) -> dict | None:
    """Серверный fingerprint: IP+UA+Accept-Language. Стабилен внутри TG in-app browser."""
    with _conn() as c:
        row = c.execute(
            "SELECT token, claimed_tg_id, created_at, expires_at FROM landing_leads "
            "WHERE client_fp=? ORDER BY created_at DESC LIMIT 1",
            (client_fp,),
        ).fetchone()
    if not row:
        return None
    return {"token": row[0], "claimed_tg_id": row[1], "created_at": row[2], "expires_at": row[3]}


def count_ip_leads_24h(ip_hash: str) -> int:
    """Сколько лидов с этого ip_hash за последние 24ч — для soft-limit и Turnstile challenge."""
    since = _iso_ago(1)
    with _conn() as c:
        row = c.execute(
            "SELECT count(*) FROM landing_leads WHERE ip_hash=? AND created_at >= ?",
            (ip_hash, since),
        ).fetchone()
    return int(row[0])


def claim_landing_lead(token: str, tg_id: int) -> dict | None:
    """Атомарно: UPDATE ... WHERE claimed_tg_id IS NULL. Возвращает lead если claim удался."""
    with _conn() as c:
        cur = c.execute(
            "UPDATE landing_leads SET claimed_tg_id=?, claimed_at=? "
            "WHERE token=? AND claimed_tg_id IS NULL",
            (tg_id, _now_iso(), token),
        )
        if cur.rowcount == 0:
            return None
    return get_landing_lead(token)


def delete_landing_lead(token: str) -> None:
    with _conn() as c:
        c.execute("DELETE FROM landing_leads WHERE token=?", (token,))


def set_landing_sub_url(token: str, sub_url: str) -> None:
    with _conn() as c:
        c.execute("UPDATE landing_leads SET sub_url=? WHERE token=?", (sub_url, token))


def record_tinkoff_pending(
    order_id: str, tg_id: int, plan_key: str, amount_kopeks: int, payment_id: str
) -> None:
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO tinkoff_payments "
            "(order_id, tg_id, plan_key, amount_kopeks, payment_id, status, created_at) "
            "VALUES (?, ?, ?, ?, ?, 'pending', ?)",
            (order_id, tg_id, plan_key, amount_kopeks, payment_id, _now_iso()),
        )


def get_tinkoff_payment(order_id: str) -> dict | None:
    with _conn() as c:
        row = c.execute(
            "SELECT order_id, tg_id, plan_key, amount_kopeks, payment_id, status, created_at, confirmed_at "
            "FROM tinkoff_payments WHERE order_id=?",
            (order_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "order_id": row[0], "tg_id": row[1], "plan_key": row[2],
        "amount_kopeks": row[3], "payment_id": row[4], "status": row[5],
        "created_at": row[6], "confirmed_at": row[7],
    }


def mark_tinkoff_confirmed(order_id: str) -> bool:
    """Атомарно переводит pending → confirmed. Возвращает True если это первый CONFIRMED
    (нужно чтобы идемпотентно обрабатывать дубликаты webhook'ов Т-Банка)."""
    with _conn() as c:
        cur = c.execute(
            "UPDATE tinkoff_payments SET status='confirmed', confirmed_at=? "
            "WHERE order_id=? AND status != 'confirmed'",
            (_now_iso(), order_id),
        )
        return cur.rowcount > 0


def mark_tinkoff_rejected(order_id: str) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE tinkoff_payments SET status='rejected' WHERE order_id=? AND status='pending'",
            (order_id,),
        )


def create_robokassa_pending(tg_id: int, plan_key: str, amount_kopeks: int) -> int:
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO robokassa_payments (tg_id, plan_key, amount_kopeks, status, created_at) "
            "VALUES (?, ?, ?, 'pending', ?)",
            (tg_id, plan_key, amount_kopeks, _now_iso()),
        )
        return cur.lastrowid


def get_robokassa_payment(inv_id: int) -> dict | None:
    with _conn() as c:
        row = c.execute(
            "SELECT inv_id, tg_id, plan_key, amount_kopeks, status, created_at, confirmed_at, activated_at "
            "FROM robokassa_payments WHERE inv_id=?",
            (inv_id,),
        ).fetchone()
    if not row:
        return None
    return {
        "inv_id": row[0], "tg_id": row[1], "plan_key": row[2],
        "amount_kopeks": row[3], "status": row[4],
        "created_at": row[5], "confirmed_at": row[6], "activated_at": row[7],
    }


def mark_robokassa_confirmed(inv_id: int) -> bool:
    """Атомарно pending → confirmed. True только при первом успешном переходе
    (idempotency — Robokassa может ретраить webhook)."""
    with _conn() as c:
        cur = c.execute(
            "UPDATE robokassa_payments SET status='confirmed', confirmed_at=? "
            "WHERE inv_id=? AND status != 'confirmed'",
            (_now_iso(), inv_id),
        )
        return cur.rowcount > 0


def mark_robokassa_activated(inv_id: int) -> bool:
    """Помечает оплату как реально активированную в Marzban. True если запись изменилась."""
    with _conn() as c:
        cur = c.execute(
            "UPDATE robokassa_payments SET activated_at=? WHERE inv_id=? AND activated_at IS NULL",
            (_now_iso(), inv_id),
        )
        return cur.rowcount > 0


def cleanup_stale_robokassa_pending(older_than_hours: int = 24) -> int:
    """Удаляет pending-оплаты старше N часов. Robokassa-сессия тайм-аутит платёж
    задолго до этого, так что pending запись после 24ч — это юзер закрыл вкладку
    и ушёл. Возвращает число удалённых записей."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=older_than_hours)).isoformat()
    with _conn() as c:
        cur = c.execute(
            "DELETE FROM robokassa_payments WHERE status='pending' AND created_at < ?",
            (cutoff,),
        )
        return cur.rowcount


def get_unactivated_robokassa_payments() -> list[dict]:
    """Подтверждённые оплаты, для которых ещё не успешно активировали Marzban.
    Используется на старте landing для добивания зависших активаций."""
    with _conn() as c:
        rows = c.execute(
            "SELECT inv_id, tg_id, plan_key, amount_kopeks, created_at, confirmed_at "
            "FROM robokassa_payments WHERE status='confirmed' AND activated_at IS NULL "
            "ORDER BY inv_id"
        ).fetchall()
    return [
        {"inv_id": r[0], "tg_id": r[1], "plan_key": r[2],
         "amount_kopeks": r[3], "created_at": r[4], "confirmed_at": r[5]}
        for r in rows
    ]


def get_users_with_mz() -> list[dict]:
    """Все зарегистрированные юзеры, у которых есть привязка к Marzban-профилю.
    Для фонового цикла напоминаний об истечении подписки."""
    with _conn() as c:
        rows = c.execute(
            "SELECT tg_id, mz_username FROM users WHERE mz_username IS NOT NULL"
        ).fetchall()
    return [{"tg_id": r[0], "mz_username": r[1]} for r in rows]


def delete_reminder_events(tg_id: int) -> None:
    """Сбрасывает reminder_* события юзера — при продлении подписки, чтобы новые
    напоминания об истечении отправились когда срок опять подойдёт."""
    with _conn() as c:
        c.execute(
            "DELETE FROM events WHERE tg_id=? AND name LIKE 'reminder_%'",
            (tg_id,),
        )


def count_events(tg_id: int, name: str) -> int:
    """Сколько раз у юзера был ивент с таким именем за всё время."""
    with _conn() as c:
        row = c.execute(
            "SELECT count(*) FROM events WHERE tg_id=? AND name=?",
            (tg_id, name),
        ).fetchone()
    return int(row[0]) if row else 0


def event_counts(names: list[str], days: int) -> dict[str, int]:
    """Count events by name in the window. Useful for funnel analysis."""
    if not names:
        return {}
    placeholders = ",".join("?" * len(names))
    with _conn() as c:
        rows = c.execute(
            f"SELECT name, count(*) FROM events "
            f"WHERE name IN ({placeholders}) AND ts >= ? GROUP BY name",
            (*names, _iso_ago(days)),
        ).fetchall()
    result = {n: 0 for n in names}
    for name, cnt in rows:
        result[name] = int(cnt)
    return result
