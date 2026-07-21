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
        # Привязка браузера к Telegram-аккаунту (Этап 4 support-чата).
        # browser_id — стабильный localStorage-токен браузера (rs_device_id);
        # fingerprint — резервный матч, если localStorage очищен.
        # Один браузер = один аккаунт (browser_id — PK); привязка к другому
        # аккаунту перезаписывает старую. Связь юзер→браузеры остаётся one-to-many
        # (несколько строк с одним tg_id, разными browser_id).
        # confirmed: 0 — привязка предполагается, 1 — юзер подтвердил, -1 — отверг.
        c.execute("""
            CREATE TABLE IF NOT EXISTS browser_links (
                browser_id  TEXT PRIMARY KEY,
                tg_id       INTEGER NOT NULL,
                fingerprint TEXT,
                source      TEXT,
                confirmed   INTEGER NOT NULL DEFAULT 0,
                linked_at   TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS ix_browser_links_fp ON browser_links(fingerprint)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_browser_links_tg ON browser_links(tg_id)")
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

        # Запросы подписки — источник для учёта устройств на одну ссылку.
        # Точка контроля наша (лендинг проксирует /sub/), панель этого не даёт:
        # HWID-лимит Remnawave — enforce БЕЗ observe-режима (клиент без x-hwid
        # получит 404), поэтому считаем сами и никого не блокируем.
        # IP хранится только хешем с солью (как в landing_leads).
        c.execute("""
            CREATE TABLE IF NOT EXISTS sub_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER,
                mz_username TEXT,
                ip_hash TEXT NOT NULL,
                user_agent TEXT,
                platform TEXT,
                hwid TEXT,
                extra_headers TEXT,
                ts TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS ix_sub_req_tg_ts ON sub_requests(tg_id, ts)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_sub_req_mz_ts ON sub_requests(mz_username, ts)")

        # Онлайн-IP с нод (Xray statsUserOnline → ip-control API панели). В отличие
        # от sub_requests считает не «кто скачал подписку», а «кто гонит трафик»:
        # клиент тут ни при чём, подделать нечем. Копим для КАЛИБРОВКИ — сколько
        # подсетей/ASN у нормального юзера, — а не для блокировок.
        #
        # Уникальные (юзер, IP) с накоплением: сырой IP не храним (только хеш с
        # солью), но держим /24 и ASN — на них строится группировка, иначе телефон,
        # скачущий по мобильной сети, считался бы тремя устройствами.
        c.execute("""
            CREATE TABLE IF NOT EXISTS online_ips (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                username TEXT,
                ip_hash TEXT NOT NULL,
                ip_prefix TEXT NOT NULL,
                asn TEXT,
                country TEXT,
                node TEXT,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                samples INTEGER NOT NULL DEFAULT 1
            )
        """)
        c.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_online_ips_user_ip "
            "ON online_ips(user_id, ip_hash)"
        )
        c.execute("CREATE INDEX IF NOT EXISTS ix_online_ips_last ON online_ips(last_seen)")

        # Снимок на каждый тик: сколько РАЗНЫХ IP/подсетей у юзера было
        # одновременно. Уникальные пары (выше) этого не покажут — там телефон и
        # ноутбук за месяц выглядят так же, как два одновременных клиента.
        c.execute("""
            CREATE TABLE IF NOT EXISTS online_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                user_id TEXT NOT NULL,
                username TEXT,
                n_ips INTEGER NOT NULL,
                n_prefixes INTEGER NOT NULL,
                n_asns INTEGER NOT NULL,
                nodes TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS ix_online_snap_ts ON online_snapshots(ts)")
        c.execute(
            "CREATE INDEX IF NOT EXISTS ix_online_snap_user ON online_snapshots(user_id, ts)"
        )

        # Лог рассылок. `kind`: transactional (истечение, оплата, инцидент — их ждут,
        # под месячный кап не попадают) | marketing (реферал, win-back, апдейт —
        # не чаще 1 в месяц на юзера).
        c.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                campaign TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'marketing',
                sent_at TEXT NOT NULL,
                meta TEXT
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS ix_notif_tg_campaign ON notifications(tg_id, campaign)")
        c.execute("CREATE INDEX IF NOT EXISTS ix_notif_kind_sent ON notifications(kind, sent_at)")

        # Блок бота юзером = 403 навсегда: канал доставки конфигов и поддержки потерян.
        # Поэтому даём выход мягче блока (opt-out) и перестаём писать заблокировавшим.
        ucols = {r[1] for r in c.execute("PRAGMA table_info(users)").fetchall()}
        if "notify_opt_out" not in ucols:
            c.execute("ALTER TABLE users ADD COLUMN notify_opt_out INTEGER NOT NULL DEFAULT 0")
        if "bot_blocked" not in ucols:
            c.execute("ALTER TABLE users ADD COLUMN bot_blocked INTEGER NOT NULL DEFAULT 0")
        # Имя (не @username) — опознание в поддержке тех, у кого ника нет вовсе:
        # по нику их не найти, а «просто tg_id» support-оператору ничего не говорит.
        if "first_name" not in ucols:
            c.execute("ALTER TABLE users ADD COLUMN first_name TEXT")


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


def set_username(tg_id: int, username: str | None) -> bool:
    """Обновить сохранённый TG-username. → True при реальном изменении.

    Зачем: record_user пишет ник через INSERT OR IGNORE — один раз при первом /start
    и больше никогда. Ник — ключ самообслуживания оплаты (find_user_by_username) и
    опознания в поддержке. Поэтому:

    * Ник СНЯТ (пусто/None) — НЕ обнуляем: оставляем ПОСЛЕДНИЙ известный, чтобы юзер/
      поддержка ещё могли найти аккаунт. Актуальность гарантирует пункт ниже.
    * Ник ЗАДАН — записываем; и «отбираем» его у другого нашего юзера, за которым он
      числился устаревшей записью (человек снял @foo → кто-то другой занял @foo):
      Telegram-ники глобально уникальны, значит текущий владелец — этот tg_id, а у
      прежнего ник обнуляем, иначе поиск увёл бы не к тому.

    Храним без ведущего '@'; регистр не нормализуем (поиск и так через LOWER()).
    """
    uname = (username or "").lstrip("@").strip() or None
    if not uname:
        return False  # ник снят — храним последний известный, не затираем
    with _conn() as c:
        row = c.execute("SELECT username FROM users WHERE tg_id=?", (tg_id,)).fetchone()
        current = row[0] if row else None
        if current is not None and current.lower() == uname.lower():
            return False  # без изменений — hot-path no-op, ничего не пишем
        # Отбираем ник у прежнего владельца (устаревшая запись за другим tg_id).
        c.execute(
            "UPDATE users SET username=NULL WHERE LOWER(username)=LOWER(?) AND tg_id<>?",
            (uname, tg_id),
        )
        c.execute("UPDATE users SET username=? WHERE tg_id=?", (uname, tg_id))
        return True


def set_first_name(tg_id: int, first_name: str | None) -> bool:
    """Сохранить/обновить имя (не @username). → True при изменении.

    В отличие от username, имя — не поисковый ключ, а атрибут опознания в поддержке
    (для тех, у кого ника нет). Коллизий/«отбора» нет — просто держим свежим. Пишем
    только при изменении (hot-path)."""
    name = (first_name or "").strip() or None
    with _conn() as c:
        cur = c.execute(
            "UPDATE users SET first_name=? "
            "WHERE tg_id=? AND COALESCE(first_name,'') <> COALESCE(?,'')",
            (name, tg_id, name),
        )
        return cur.rowcount > 0


def get_user_identity(tg_id: int) -> dict:
    """{username, first_name} — для карточки поддержки (опознать человека)."""
    with _conn() as c:
        row = c.execute(
            "SELECT username, first_name FROM users WHERE tg_id=?", (tg_id,)
        ).fetchone()
    return ({"username": row[0], "first_name": row[1]} if row
            else {"username": None, "first_name": None})


def all_user_ids() -> list[int]:
    """Все tg_id — для фонового синка username (getChat по каждому)."""
    with _conn() as c:
        return [int(r[0]) for r in c.execute("SELECT tg_id FROM users").fetchall()]


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
        # Покупка сбрасывает win-back-цикл: после истечения купленной подписки он
        # начнётся заново (+30 → 3-дн триал → +60). Только тут — бонусы платежом
        # не считаются и цикл не рвут. В той же транзакции (без вложенного _conn).
        c.execute(
            "DELETE FROM notifications WHERE tg_id=? AND campaign IN ('win_back_30', 'win_back_90')",
            (tg_id,),
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


# ── Browser ↔ account links (Этап 4 support-чата) ────────────────────────────

def link_browser(browser_id: str, tg_id: int, fingerprint: str | None = None,
                  source: str | None = None) -> None:
    """Привязывает браузер к Telegram-аккаунту (один браузер — один аккаунт).
    Повторный вызов с тем же аккаунтом обновляет fingerprint/source/linked_at;
    confirmed=MAX(confirmed,0) — «это я» (1) сохраняется, а «не я» (-1) снимается:
    осознанный повторный заход из бота на свой аккаунт отменяет прошлый отказ
    (юзер мог нажать «не я», оплачивая чужой аккаунт, и вернуться к своему).
    Привязка к ДРУГОМУ аккаунту перезаписывает строку и сбрасывает confirmed."""
    if not browser_id or not tg_id:
        return
    with _conn() as c:
        row = c.execute(
            "SELECT tg_id FROM browser_links WHERE browser_id=?", (browser_id,)
        ).fetchone()
        if row is None:
            c.execute(
                "INSERT INTO browser_links "
                "(browser_id, tg_id, fingerprint, source, confirmed, linked_at) "
                "VALUES (?, ?, ?, ?, 0, ?)",
                (browser_id, tg_id, fingerprint, source, _now_iso()),
            )
        elif row[0] == tg_id:
            c.execute(
                "UPDATE browser_links SET fingerprint=COALESCE(?, fingerprint), "
                "source=COALESCE(?, source), linked_at=?, "
                "confirmed=MAX(confirmed, 0) WHERE browser_id=?",
                (fingerprint, source, _now_iso(), browser_id),
            )
        else:
            c.execute(
                "UPDATE browser_links SET tg_id=?, fingerprint=?, source=?, "
                "confirmed=0, linked_at=? WHERE browser_id=?",
                (tg_id, fingerprint, source, _now_iso(), browser_id),
            )


def set_browser_link_confirmed(browser_id: str, tg_id: int, confirmed: int) -> bool:
    """confirmed: 1 — юзер подтвердил «это я», -1 — «это не я». True если строка изменилась."""
    with _conn() as c:
        cur = c.execute(
            "UPDATE browser_links SET confirmed=? WHERE browser_id=? AND tg_id=?",
            (confirmed, browser_id, tg_id),
        )
        return cur.rowcount > 0


def get_browser_accounts(browser_id: str, fingerprint: str | None = None) -> list[dict]:
    """Аккаунты, привязанные к браузеру. Сначала точный матч по browser_id,
    при пустом результате — резервный по fingerprint. Отвергнутые (confirmed=-1)
    исключаются. Подтверждённые и свежие — первыми."""
    rows: list = []
    with _conn() as c:
        if browser_id:
            rows = c.execute(
                "SELECT tg_id, confirmed, source, linked_at FROM browser_links "
                "WHERE browser_id=? AND confirmed != -1 "
                "ORDER BY confirmed DESC, linked_at DESC",
                (browser_id,),
            ).fetchall()
        if not rows and fingerprint:
            rows = c.execute(
                "SELECT tg_id, confirmed, source, linked_at FROM browser_links "
                "WHERE fingerprint=? AND confirmed != -1 "
                "ORDER BY confirmed DESC, linked_at DESC",
                (fingerprint,),
            ).fetchall()
    return [{"tg_id": r[0], "confirmed": r[1], "source": r[2], "linked_at": r[3]}
            for r in rows]


def get_browsers_for_tg(tg_id: int) -> list[str]:
    """Все browser_id, привязанные к этому tg_id (среди не-отвергнутых).
    Используется support-bot для поиска веб-тредов одного аккаунта,
    созданных с разных браузеров (фича A — единый чат)."""
    with _conn() as c:
        rows = c.execute(
            "SELECT browser_id FROM browser_links "
            "WHERE tg_id=? AND confirmed != -1",
            (tg_id,),
        ).fetchall()
    return [r[0] for r in rows]


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


def log_sub_request(
    tg_id: int | None,
    mz_username: str | None,
    ip_hash: str,
    user_agent: str | None,
    platform: str | None,
    hwid: str | None,
    extra_headers: str | None = None,
) -> None:
    """Пишет факт запроса подписки — сырьё для учёта устройств."""
    with _conn() as c:
        c.execute(
            "INSERT INTO sub_requests "
            "(tg_id, mz_username, ip_hash, user_agent, platform, hwid, extra_headers, ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (tg_id, mz_username, ip_hash, user_agent, platform, hwid, extra_headers, _now_iso()),
        )


def device_stats(days: int = 7) -> list[dict]:
    """Оценка числа устройств на подписку за окно.

    Точный счётчик даёт только hwid (его шлёт не всякий клиент), поэтому
    отдаём обе метрики: distinct hwid и эвристику distinct (ip_hash, platform).
    Эвристика ЗАВЫШАЕТ — один телефон скачет Wi-Fi↔мобильная сеть, — поэтому
    годится как детектор перепродажи, а не как счётчик устройств.
    """
    with _conn() as c:
        rows = c.execute(
            "SELECT COALESCE(CAST(tg_id AS TEXT), mz_username) AS who, "
            "       COUNT(DISTINCT hwid) AS hwids, "
            "       COUNT(DISTINCT ip_hash || '|' || COALESCE(platform, '?')) AS ip_platforms, "
            "       COUNT(DISTINCT platform) AS platforms, "
            "       COUNT(*) AS requests, "
            "       MAX(ts) AS last_seen "
            "FROM sub_requests WHERE ts >= ? GROUP BY who ORDER BY ip_platforms DESC",
            (_iso_ago(days),),
        ).fetchall()
    return [
        {
            "who": r[0],
            "hwids": int(r[1]),
            "ip_platforms": int(r[2]),
            "platforms": int(r[3]),
            "requests": int(r[4]),
            "last_seen": r[5],
        }
        for r in rows
    ]


def upsert_online_ip(
    user_id: str,
    username: str | None,
    ip_hash: str,
    ip_prefix: str,
    asn: str | None,
    country: str | None,
    node: str | None,
) -> None:
    """Копит уникальные (юзер, IP): первый раз — INSERT, дальше растит samples."""
    now = _now_iso()
    with _conn() as c:
        c.execute(
            "INSERT INTO online_ips "
            "(user_id, username, ip_hash, ip_prefix, asn, country, node, first_seen, last_seen) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(user_id, ip_hash) DO UPDATE SET "
            "  last_seen = excluded.last_seen, "
            "  samples = samples + 1, "
            "  node = excluded.node, "
            # ASN/страна могли не разрезолвиться на первом тике — дозаполняем.
            "  asn = COALESCE(excluded.asn, asn), "
            "  country = COALESCE(excluded.country, country)",
            (user_id, username, ip_hash, ip_prefix, asn, country, node, now, now),
        )


def log_online_snapshot(
    user_id: str,
    username: str | None,
    n_ips: int,
    n_prefixes: int,
    n_asns: int,
    nodes: str | None,
) -> None:
    """Одновременность на момент тика — по ней и выставляются пороги."""
    with _conn() as c:
        c.execute(
            "INSERT INTO online_snapshots "
            "(ts, user_id, username, n_ips, n_prefixes, n_asns, nodes) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (_now_iso(), user_id, username, n_ips, n_prefixes, n_asns, nodes),
        )


def online_ip_stats(days: int = 7) -> list[dict]:
    """Профиль юзера за окно: уникальные IP / подсети / ASN + пик одновременных.

    `peak_prefixes` берётся из снимков — это и есть «сколько мест сразу», в
    отличие от `prefixes` (сколько мест за всё окно: дом, работа, поезд).
    """
    since = _iso_ago(days)
    with _conn() as c:
        rows = c.execute(
            "SELECT user_id, "
            "       COALESCE(MAX(username), user_id) AS username, "
            "       COUNT(*) AS ips, "
            "       COUNT(DISTINCT ip_prefix) AS prefixes, "
            "       COUNT(DISTINCT asn) AS asns, "
            "       COUNT(DISTINCT country) AS countries, "
            "       MAX(last_seen) AS last_seen "
            "FROM online_ips WHERE last_seen >= ? GROUP BY user_id",
            (since,),
        ).fetchall()
        peaks = dict(
            c.execute(
                "SELECT user_id, MAX(n_prefixes) FROM online_snapshots "
                "WHERE ts >= ? GROUP BY user_id",
                (since,),
            ).fetchall()
        )
    return [
        {
            "user_id": r[0],
            "username": r[1],
            "ips": int(r[2]),
            "prefixes": int(r[3]),
            "asns": int(r[4]),
            "countries": int(r[5]),
            "peak_prefixes": int(peaks.get(r[0], 0)),
            "last_seen": r[6],
        }
        for r in sorted(rows, key=lambda x: -x[3])
    ]


def log_notification(tg_id: int, campaign: str, kind: str = "marketing", meta: dict | None = None) -> None:
    with _conn() as c:
        c.execute(
            "INSERT INTO notifications (tg_id, campaign, kind, sent_at, meta) VALUES (?, ?, ?, ?, ?)",
            (tg_id, campaign, kind, _now_iso(), json.dumps(meta or {}, ensure_ascii=False)),
        )


def was_notified(tg_id: int, campaign: str) -> bool:
    """Кампания уже уходила этому юзеру (идемпотентность рассылок)."""
    with _conn() as c:
        row = c.execute(
            "SELECT 1 FROM notifications WHERE tg_id=? AND campaign=? LIMIT 1", (tg_id, campaign)
        ).fetchone()
    return row is not None


def notification_count(tg_id: int, campaign: str) -> int:
    """Сколько раз кампания уходила юзеру. Нужно повторяющимся трекам (не `once`),
    где вариант текста и предел отправок выбираются по счётчику."""
    with _conn() as c:
        row = c.execute(
            "SELECT count(*) FROM notifications WHERE tg_id=? AND campaign=?", (tg_id, campaign)
        ).fetchone()
    return int(row[0]) if row else 0


def has_sub_request(tg_id: int) -> bool:
    """Юзер хоть раз утянул конфиг подписки.

    Ноль запросов при активированном триале = застрял на установке: ссылку выдали,
    но клиент за ней не пришёл. Обратное неверно — конфиг тянется один раз и дальше
    живёт в приложении, так что наличие запроса НЕ значит, что человек пользуется.
    """
    with _conn() as c:
        row = c.execute("SELECT 1 FROM sub_requests WHERE tg_id=? LIMIT 1", (tg_id,)).fetchone()
    return row is not None


def marketing_sent_since(tg_id: int, days: int) -> int:
    """Сколько маркетинговых сообщений ушло юзеру за окно — для месячного капа."""
    with _conn() as c:
        row = c.execute(
            "SELECT count(*) FROM notifications WHERE tg_id=? AND kind='marketing' AND sent_at >= ?",
            (tg_id, _iso_ago(days)),
        ).fetchone()
    return int(row[0]) if row else 0


def notification_age_days(tg_id: int, campaign: str) -> float | None:
    """Сколько дней прошло с последней отправки кампании юзеру (None — не слали).

    Нужно win-back-циклу: второе касание идёт через фикс. срок после первого.
    julianday корректно парсит наш ISO с tz-оффсетом и микросекундами.
    """
    with _conn() as c:
        row = c.execute(
            "SELECT julianday('now') - julianday(MAX(sent_at)) FROM notifications "
            "WHERE tg_id=? AND campaign=?",
            (tg_id, campaign),
        ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def delete_winback_notifications(tg_id: int) -> None:
    """Сброс win-back-цикла при покупке: снимаем отметки win_back_*, чтобы после
    истечения купленной подписки цикл (+30 → 3-дн триал → +60) начался заново."""
    with _conn() as c:
        c.execute(
            "DELETE FROM notifications WHERE tg_id=? AND campaign IN ('win_back_30', 'win_back_90')",
            (tg_id,),
        )


def set_notify_opt_out(tg_id: int, value: bool = True) -> None:
    with _conn() as c:
        c.execute("UPDATE users SET notify_opt_out=? WHERE tg_id=?", (1 if value else 0, tg_id))


def mark_bot_blocked(tg_id: int, value: bool = True) -> None:
    """403 от Telegram = юзер заблокировал бота — записываем факт (для статистики/
    опознания в поддержке). НЕ исключаем его из будущих рассылок: другой пуш через
    день/неделю всё равно надо попытаться доставить (вдруг уже разблокировал). Чтобы
    не долбить ОДНИМ И ТЕМ ЖЕ пушем — попытка помечается в notifications
    (см. campaigns.send), а не флагом блока."""
    with _conn() as c:
        c.execute("UPDATE users SET bot_blocked=? WHERE tg_id=?", (1 if value else 0, tg_id))


def get_reachable_users() -> list[dict]:
    """Кому можно писать: все, кто не отписался от рассылок (opt-out).

    Заблокировавших бота НЕ прячем: повтор одного пуша душит идемпотентность
    (was_notified в eligible()), а другие/будущие кампании к ним пойдут как обычно —
    человек мог разблокировать. Единственный жёсткий стоп — явный opt-out.
    """
    with _conn() as c:
        rows = c.execute(
            "SELECT tg_id, username, mz_username, first_seen, last_seen, trial_activated "
            "FROM users WHERE COALESCE(notify_opt_out,0)=0"
        ).fetchall()
    return [
        {"tg_id": r[0], "username": r[1], "mz_username": r[2],
         "first_seen": r[3], "last_seen": r[4], "trial_activated": r[5]}
        for r in rows
    ]


def get_paying_user_ids() -> set[int]:
    with _conn() as c:
        rows = c.execute("SELECT DISTINCT tg_id FROM payments").fetchall()
    return {int(r[0]) for r in rows}


def payment_counts_by_user() -> dict[int, int]:
    """tg_id → сколько раз платил. Нужно для реферального анлока (≥2 платежа)."""
    with _conn() as c:
        rows = c.execute("SELECT tg_id, count(*) FROM payments GROUP BY tg_id").fetchall()
    return {int(r[0]): int(r[1]) for r in rows}


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
