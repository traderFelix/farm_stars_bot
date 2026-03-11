import aiosqlite, uuid, asyncio
from typing import Optional, List, Tuple
from contextlib import asynccontextmanager
from config import DB_PATH
from decimal import Decimal, ROUND_DOWN


# ---------- Connection / TX ----------

async def open_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(
        DB_PATH,
        timeout=30,
        isolation_level=None,  # важно
    )
    db.row_factory = aiosqlite.Row

    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA foreign_keys=ON;")
    await db.execute("PRAGMA busy_timeout=30000;")

    db._tx_lock = asyncio.Lock()

    return db

async def close_db(db: aiosqlite.Connection) -> None:
    await db.close()

@asynccontextmanager
async def tx(db: aiosqlite.Connection, immediate: bool = True):
    if getattr(db, "in_transaction", False):
        sp_name = f"sp_{uuid.uuid4().hex}"
        await db.execute(f'SAVEPOINT "{sp_name}"')
        try:
            yield
            await db.execute(f'RELEASE SAVEPOINT "{sp_name}"')
        except Exception:
            await db.execute(f'ROLLBACK TO SAVEPOINT "{sp_name}"')
            await db.execute(f'RELEASE SAVEPOINT "{sp_name}"')
            raise
    else:
        async with db._tx_lock:  # type: ignore[attr-defined]
            await db.execute("BEGIN IMMEDIATE;" if immediate else "BEGIN;")
            try:
                yield
                await db.commit()
            except Exception:
                await db.rollback()
                raise


async def init_db(db: aiosqlite.Connection) -> None:
    await db.executescript("""
    CREATE TABLE IF NOT EXISTS users (
      user_id INTEGER PRIMARY KEY,
      username TEXT,
      tg_first_name TEXT,
      tg_last_name TEXT,
      balance NUMERIC DEFAULT 0 CHECK(balance >= 0),
      is_suspicious INTEGER NOT NULL DEFAULT 0,
      suspicious_reason TEXT,
      is_banned INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      last_seen_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS campaigns (
      campaign_key TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      reward_amount NUMERIC NOT NULL,
      status TEXT DEFAULT 'draft',             -- draft | active | ended
      description TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      starts_at TEXT,
      ends_at TEXT
    );

    CREATE TABLE IF NOT EXISTS claims (
      user_id INTEGER NOT NULL,
      campaign_key TEXT NOT NULL,
      amount NUMERIC NOT NULL,
      claimed_at TEXT DEFAULT (datetime('now')),
      status TEXT DEFAULT 'ok',
      PRIMARY KEY (user_id, campaign_key),
      FOREIGN KEY (user_id) REFERENCES users(user_id),
      FOREIGN KEY (campaign_key) REFERENCES campaigns(campaign_key)
    );

    CREATE TABLE IF NOT EXISTS campaign_winners (
      campaign_key TEXT NOT NULL,
      username TEXT NOT NULL,                 -- храним БЕЗ @
      user_id INTEGER,                        -- подтянем позже, когда победитель зайдет
      added_at TEXT DEFAULT (datetime('now')),
      added_by INTEGER,
      PRIMARY KEY (campaign_key, username),
      FOREIGN KEY (campaign_key) REFERENCES campaigns(campaign_key)
    );

    CREATE TABLE IF NOT EXISTS ledger (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      delta NUMERIC NOT NULL,
      reason TEXT NOT NULL,                    -- withdraw_hold | withdraw_paid | withdraw_release | admin_adjust | contest_bonus
      campaign_key TEXT,
      withdrawal_id INTEGER,
      meta TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(user_id)
    );

    CREATE TABLE IF NOT EXISTS withdrawals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      amount NUMERIC NOT NULL,
      method TEXT NOT NULL,                    -- 'ton' | 'stars'
      details TEXT,                            -- wallet address for TON
      status TEXT NOT NULL DEFAULT 'pending',  -- pending|paid|rejected
      created_at TEXT DEFAULT (datetime('now')),
      processed_at TEXT,
      processed_by INTEGER,
      fee_xtr INTEGER NOT NULL DEFAULT 0,
      fee_paid INTEGER NOT NULL DEFAULT 0,
      fee_refunded INTEGER NOT NULL DEFAULT 0,
      fee_telegram_charge_id TEXT,
      fee_invoice_payload TEXT,
      FOREIGN KEY (user_id) REFERENCES users(user_id)
    );

    CREATE TABLE IF NOT EXISTS abuse_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      action TEXT NOT NULL,                       -- claim_click | claim_fail | withdraw_create
      amount NUMERIC DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    );
    
    CREATE TABLE IF NOT EXISTS xtr_ledger (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      withdrawal_id INTEGER,
      delta_xtr INTEGER NOT NULL,                  -- + списали комиссию / - вернули комиссию
      reason TEXT NOT NULL,                        -- withdraw_fee_paid | withdraw_fee_refunded | admin_fee_refund
      telegram_payment_charge_id TEXT,
      invoice_payload TEXT,
      meta TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(user_id),
      FOREIGN KEY (withdrawal_id) REFERENCES withdrawals(id)
    );
    
    CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at);
    CREATE INDEX IF NOT EXISTS idx_users_last_seen_at ON users(last_seen_at);
    CREATE INDEX IF NOT EXISTS idx_campaigns_status_created ON campaigns(status, created_at);
    CREATE INDEX IF NOT EXISTS idx_claims_campaign_key ON claims(campaign_key);
    CREATE INDEX IF NOT EXISTS idx_winners_campaign_key ON campaign_winners(campaign_key);
    CREATE INDEX IF NOT EXISTS idx_ledger_user_created ON ledger(user_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_ledger_withdrawal ON ledger(withdrawal_id);
    CREATE INDEX IF NOT EXISTS idx_withdrawals_status_created ON withdrawals(status, created_at);
    CREATE INDEX IF NOT EXISTS idx_withdrawals_user_created ON withdrawals(user_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_abuse_events_user_action_time ON abuse_events(user_id, action, created_at);
    CREATE INDEX IF NOT EXISTS idx_xtr_ledger_reason_created ON xtr_ledger(reason, created_at);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_xtr_ledger_unique_paid_charge ON xtr_ledger(reason, telegram_payment_charge_id)
      WHERE reason = 'withdraw_fee_paid' AND telegram_payment_charge_id IS NOT NULL;
    """)
    await db.commit()


# ---------- Users ----------

async def register_user(
        db: aiosqlite.Connection,
        user_id: int,
        username: Optional[str],
        first_name: Optional[str] = None,
        last_name: Optional[str] = None
) -> None:
    u = (username or "").strip().lstrip("@") or None
    fn = (first_name or "").strip() or None
    ln = (last_name or "").strip() or None

    async with db.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,)) as cur:
        exists = await cur.fetchone() is not None

    if not exists:
        await db.execute(
            """
            INSERT INTO users (user_id, username, tg_first_name, tg_last_name, balance, created_at, last_seen_at)
            VALUES (?, ?, ?, ?, 0, datetime('now'), datetime('now'))
            """,
            (user_id, u, fn, ln),
        )
        return

    await db.execute(
        """
        UPDATE users
        SET username = COALESCE(?, username),
            tg_first_name = COALESCE(?, tg_first_name),
            tg_last_name = COALESCE(?, tg_last_name),
            last_seen_at = datetime('now')
        WHERE user_id = ?
        """,
        (u, fn, ln, user_id),
    )

async def ensure_user_registered(message_or_callback, db):
    user = message_or_callback.from_user
    async with tx(db, immediate=False):
        await register_user(
            db,
            user.id,
            user.username,
            user.first_name,
            user.last_name,
        )

async def get_balance(db: aiosqlite.Connection, user_id: int) -> float:
    async with db.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,)) as cur:
        row = await cur.fetchone()
    return float(row["balance"]) if row else 0.0

async def total_balances(db: aiosqlite.Connection) -> float:
    async with db.execute("SELECT COALESCE(SUM(balance), 0) AS s FROM users") as cur:
        row = await cur.fetchone()
    return float(row["s"] or 0.0)

async def top_users_by_balance(db: aiosqlite.Connection, limit: int = 10):
    async with db.execute(
            """
        SELECT username, balance
        FROM users
        ORDER BY balance DESC
        LIMIT ?
        """,
            (int(limit),),
    ) as cur:
        return await cur.fetchall()


# ---------- Campaigns ----------

async def upsert_campaign(
        db: aiosqlite.Connection,
        campaign_key: str,
        title: str,
        reward_amount: float,
        status: str = "draft",
        description: Optional[str] = None,
) -> None:
    await db.execute(
        """
        INSERT INTO campaigns (campaign_key, title, reward_amount, status, description)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(campaign_key) DO UPDATE SET
            title=excluded.title,
            reward_amount=excluded.reward_amount,
            status=excluded.status,
            description=excluded.description
        """,
        (campaign_key, title, float(reward_amount), status, description),
    )

async def set_campaign_status(db: aiosqlite.Connection, campaign_key: str, status: str) -> None:
    await db.execute(
        "UPDATE campaigns SET status = ? WHERE campaign_key = ?",
        (status, campaign_key),
    )

async def delete_campaign(db: aiosqlite.Connection, campaign_key: str) -> None:
    await db.execute("DELETE FROM claims WHERE campaign_key = ?", (campaign_key,))
    await db.execute("DELETE FROM campaign_winners WHERE campaign_key = ?", (campaign_key,))
    await db.execute("DELETE FROM campaigns WHERE campaign_key = ?", (campaign_key,))

async def get_campaign(db: aiosqlite.Connection, campaign_key: str):
    async with db.execute(
            "SELECT campaign_key, title, reward_amount, status FROM campaigns WHERE campaign_key = ?",
            (campaign_key,),
    ) as cur:
        return await cur.fetchone()

async def list_campaigns(db: aiosqlite.Connection):
    async with db.execute(
            """
        SELECT campaign_key, reward_amount, status, created_at
        FROM campaigns
        ORDER BY datetime(created_at) DESC
        """
    ) as cur:
        return await cur.fetchall()

async def list_campaigns_latest(db: aiosqlite.Connection, limit: int = 5):
    async with db.execute(
            """
        SELECT campaign_key, reward_amount, status, created_at
        FROM campaigns
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
            (int(limit),),
    ) as cur:
        return await cur.fetchall()

async def list_active_campaigns(db: aiosqlite.Connection):
    async with db.execute(
            """
        SELECT campaign_key, title, reward_amount
        FROM campaigns
        WHERE status = 'active'
        ORDER BY datetime(created_at) DESC
        """
    ) as cur:
        return await cur.fetchall()

async def campaigns_status_counts(db: aiosqlite.Connection) -> Tuple[int, int, int]:
    async with db.execute("SELECT status, COUNT(*) AS cnt FROM campaigns GROUP BY status") as cur:
        rows = await cur.fetchall()

    counts = {"active": 0, "ended": 0, "draft": 0}
    for r in rows:
        counts[str(r["status"])] = int(r["cnt"])

    return counts["active"], counts["ended"], counts["draft"]


# ---------- Winners ----------

async def add_winners(db: aiosqlite.Connection, campaign_key: str, usernames: List[str]) -> int:
    count = 0
    for u in usernames:
        u = (u or "").strip().lstrip("@")
        if not u:
            continue
        await db.execute(
            "INSERT OR IGNORE INTO campaign_winners (campaign_key, username) VALUES (?, ?)",
            (campaign_key, u),
        )
        count += 1
    return count

async def list_winners(db: aiosqlite.Connection, campaign_key: str) -> List[str]:
    async with db.execute(
            "SELECT username FROM campaign_winners WHERE campaign_key = ? ORDER BY added_at ASC",
            (campaign_key,),
    ) as cur:
        rows = await cur.fetchall()
    return [r["username"] for r in rows]

async def winners_count(db: aiosqlite.Connection, campaign_key: str) -> int:
    async with db.execute("SELECT COUNT(*) AS c FROM campaign_winners WHERE campaign_key = ?", (campaign_key,)) as cur:
        row = await cur.fetchone()
    return int(row["c"])

async def attach_winner_user_id(db: aiosqlite.Connection, campaign_key: str, username: str, user_id: int) -> None:
    u = (username or "").strip().lstrip("@")
    if not u:
        return
    await db.execute(
        """
        UPDATE campaign_winners
        SET user_id = ?
        WHERE campaign_key = ?
          AND username = ?
          AND user_id IS NULL
        """,
        (int(user_id), campaign_key, u),
    )

async def is_winner(db: aiosqlite.Connection, campaign_key: str, user_id: int, username: Optional[str]) -> bool:
    async with db.execute(
            "SELECT 1 FROM campaign_winners WHERE campaign_key = ? AND user_id = ? LIMIT 1",
            (campaign_key, int(user_id)),
    ) as cur:
        if await cur.fetchone() is not None:
            return True

    u = (username or "").strip().lstrip("@")
    if not u:
        return False

    async with db.execute(
            "SELECT 1 FROM campaign_winners WHERE campaign_key = ? AND username = ? LIMIT 1",
            (campaign_key, u),
    ) as cur:
        return await cur.fetchone() is not None

async def delete_winner_if_not_claimed(db: aiosqlite.Connection, campaign_key: str, username: str):
    u = (username or "").strip().lstrip("@")
    if not u:
        return False, "Пустой username"

    async with db.execute(
            "SELECT user_id FROM campaign_winners WHERE campaign_key = ? AND username = ?",
            (campaign_key, u),
    ) as cur:
        row = await cur.fetchone()
    if row is None:
        return False, "Этого username нет в списке победителей"

    winner_user_id = row["user_id"]

    user_id_by_username = None
    if winner_user_id is None:
        async with db.execute("SELECT user_id FROM users WHERE username = ? LIMIT 1", (u,)) as cur:
            r2 = await cur.fetchone()
        if r2:
            user_id_by_username = r2["user_id"]

    async with db.execute(
            """
        SELECT 1
        FROM claims cl
        WHERE cl.campaign_key = ?
          AND (
            (? IS NOT NULL AND cl.user_id = ?)
            OR (? IS NOT NULL AND cl.user_id = ?)
          )
        LIMIT 1
        """,
            (campaign_key, winner_user_id, winner_user_id, user_id_by_username, user_id_by_username),
    ) as cur:
        if await cur.fetchone() is not None:
            return False, "Нельзя удалить: этот победитель уже заклеймил"

    await db.execute(
        "DELETE FROM campaign_winners WHERE campaign_key = ? AND username = ?",
        (campaign_key, u),
    )
    return True, "Удалено"


# ---------- Claims ----------

async def has_claim(db: aiosqlite.Connection, user_id: int, campaign_key: str) -> bool:
    async with db.execute(
            "SELECT 1 FROM claims WHERE user_id = ? AND campaign_key = ?",
            (int(user_id), campaign_key),
    ) as cur:
        return await cur.fetchone() is not None

async def add_claim(db: aiosqlite.Connection, user_id: int, campaign_key: str, amount: float) -> None:
    await db.execute(
        "INSERT INTO claims (user_id, campaign_key, amount) VALUES (?, ?, ?)",
        (int(user_id), campaign_key, float(amount)),
    )

async def claimed_usernames(db: aiosqlite.Connection, campaign_key: str) -> List[str]:
    async with db.execute(
            """
        SELECT u.username
        FROM claims c
        JOIN users u ON u.user_id = c.user_id
        WHERE c.campaign_key = ?
          AND u.username IS NOT NULL
          AND u.username != ''
        ORDER BY datetime(c.claimed_at) ASC
        """,
            (campaign_key,),
    ) as cur:
        rows = await cur.fetchall()
    return [r["username"] for r in rows]

async def campaign_stats(db: aiosqlite.Connection, campaign_key: str):
    async with db.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(amount), 0) AS total FROM claims WHERE campaign_key = ?",
            (campaign_key,),
    ) as cur:
        row = await cur.fetchone()
    claims_count = int(row["cnt"] or 0)
    total_paid = float(row["total"] or 0.0)

    async with db.execute("SELECT COUNT(*) AS c FROM campaign_winners WHERE campaign_key = ?", (campaign_key,)) as cur:
        r2 = await cur.fetchone()
    winners_cnt = int(r2["c"])

    return claims_count, winners_cnt, total_paid

async def global_claims_stats(db: aiosqlite.Connection):
    async with db.execute("SELECT COUNT(*) AS cnt, COALESCE(SUM(amount), 0) AS total FROM claims") as cur:
        row = await cur.fetchone()
    return int(row["cnt"] or 0), float(row["total"] or 0.0)

async def claim_reward(
        db: aiosqlite.Connection,
        user_id: int,
        username: Optional[str],
        campaign_key: str,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
) -> tuple[bool, str, float]:

    uid = int(user_id)
    ck = (campaign_key or "").strip()

    async with tx(db, immediate=True):
        await register_user(db, uid, username, first_name, last_name)

        row = await get_campaign(db, ck)
        if not row:
            return False, "❌ Конкурс не найден", 0.0

        _k, title, reward_amount, status = row[0], row[1], float(row[2]), row[3]
        if status != "active":
            return False, "❌ Этот конкурс сейчас неактивен", 0.0

        if username:
            await attach_winner_user_id(db, ck, username, uid)

        ok_winner = await is_winner(db, ck, uid, username)
        if not ok_winner:
            return False, "❌ Ты не в списке победителей этого конкурса", 0.0

        try:
            await add_claim(db, uid, ck, reward_amount)
        except Exception:
            return False, "⚠️ Ты уже забрал награду в этом конкурсе", 0.0

        await apply_balance_delta(
            db,
            user_id=uid,
            delta=reward_amount,
            reason="contest_bonus",
            campaign_key=ck,
            meta=title,
        )

        new_balance = await get_balance(db, uid)
        return True, f"✅ Ты получил {reward_amount:g}⭐️ ({title})", float(new_balance)

# ---------- Totals for admin dashboard ----------

async def total_assigned_amount(db: aiosqlite.Connection) -> float:
    async with db.execute(
            """
        SELECT COALESCE(SUM(c.reward_amount), 0) AS total
        FROM campaign_winners w
        JOIN campaigns c ON c.campaign_key = w.campaign_key
        """
    ) as cur:
        row = await cur.fetchone()
    return float(row["total"] or 0.0)

async def unclaimed_total_amount(db: aiosqlite.Connection) -> float:
    async with db.execute(
            """
        SELECT COALESCE(SUM(c.reward_amount), 0) AS total
        FROM campaign_winners w
        JOIN campaigns c ON c.campaign_key = w.campaign_key
        LEFT JOIN users u ON u.username = w.username
        WHERE NOT EXISTS (
            SELECT 1
            FROM claims cl
            WHERE cl.campaign_key = w.campaign_key
              AND (
                (w.user_id IS NOT NULL AND cl.user_id = w.user_id)
                OR (w.user_id IS NULL AND u.user_id IS NOT NULL AND cl.user_id = u.user_id)
              )
        )
        """
    ) as cur:
        row = await cur.fetchone()
    return float(row["total"] or 0.0)

async def users_total_count(db: aiosqlite.Connection) -> int:
    async with db.execute("SELECT COUNT(*) AS c FROM users") as cur:
        row = await cur.fetchone()
    return int(row["c"])

async def users_new_since_hours(db: aiosqlite.Connection, hours: int) -> int:
    async with db.execute(
            "SELECT COUNT(*) AS c FROM users WHERE created_at >= datetime('now', ?)",
            (f"-{int(hours)} hours",),
    ) as cur:
        row = await cur.fetchone()
    return int(row["c"])

async def users_new_since_days(db: aiosqlite.Connection, days: int) -> int:
    async with db.execute(
            "SELECT COUNT(*) AS c FROM users WHERE created_at >= datetime('now', ?)",
            (f"-{int(days)} days",),
    ) as cur:
        row = await cur.fetchone()
    return int(row["c"])

async def users_active_since_days(db: aiosqlite.Connection, days: int) -> int:
    async with db.execute(
            "SELECT COUNT(*) AS c FROM users WHERE last_seen_at >= datetime('now', ?)",
            (f"-{int(days)} days",),
    ) as cur:
        row = await cur.fetchone()
    return int(row["c"])

async def users_growth_by_day(db: aiosqlite.Connection, days: int = 30):
    async with db.execute(
            """
        SELECT date(created_at) AS d, COUNT(*) AS cnt
        FROM users
        WHERE created_at >= datetime('now', ?)
        GROUP BY d
        ORDER BY d ASC
        """,
            (f"-{int(days)} days",),
    ) as cur:
        rows = await cur.fetchall()
    return [(r["d"], int(r["cnt"])) for r in rows]


# ---------- Ledger / Withdrawals ----------

async def ledger_add(
        db: aiosqlite.Connection,
        user_id: int,
        delta: float,
        reason: str,
        campaign_key: Optional[str] = None,
        withdrawal_id: Optional[int] = None,
        meta: Optional[str] = None,
) -> None:
    await db.execute(
        """
        INSERT INTO ledger (user_id, delta, reason, campaign_key, withdrawal_id, meta, created_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (int(user_id), float(delta), reason, campaign_key, withdrawal_id, meta),
    )

async def ledger_last(db: aiosqlite.Connection, user_id: int, limit: int = 20):
    async with db.execute(
            """
        SELECT created_at, delta, reason, campaign_key, meta
        FROM ledger
        WHERE user_id = ?
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
            (int(user_id), int(limit)),
    ) as cur:
        return await cur.fetchall()

async def ledger_sum(db: aiosqlite.Connection, user_id: int) -> float:
    async with db.execute(
            "SELECT COALESCE(SUM(delta), 0) AS s FROM ledger WHERE user_id = ?",
            (int(user_id),),
    ) as cur:
        row = await cur.fetchone()
    return float(row["s"] or 0.0)

async def ledger_user_history(db: aiosqlite.Connection, user_id: int, limit: int = 20):
    async with db.execute(
            """
        SELECT created_at, delta, reason, campaign_key
        FROM ledger
        WHERE user_id = ?
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
            (int(user_id), int(limit)),
    ) as cur:
        return await cur.fetchall()

async def create_withdrawal(db: aiosqlite.Connection, user_id: int, amount: float, method: str, details: Optional[str] = None) -> int:
    cur = await db.execute(
        """
        INSERT INTO withdrawals (user_id, amount, method, details, status)
        VALUES (?, ?, ?, ?, 'pending')
        """,
        (int(user_id), float(amount), method, details),
    )
    return int(cur.lastrowid)

async def list_withdrawals(db: aiosqlite.Connection, status: str = "pending", limit: int = 20):
    async with db.execute(
            """
        SELECT w.id, w.user_id, u.username, w.amount, w.method, w.details, w.status, w.created_at
        FROM withdrawals w
        LEFT JOIN users u ON u.user_id = w.user_id
        WHERE w.status = ?
        ORDER BY datetime(w.created_at) DESC
        LIMIT ?
        """,
            (status, int(limit)),
    ) as cur:
        return await cur.fetchall()

async def get_withdrawal(db: aiosqlite.Connection, withdrawal_id: int):
    async with db.execute(
            """
        SELECT w.id, w.user_id, u.username, w.amount, w.method, w.details, w.status, w.created_at
        FROM withdrawals w
        LEFT JOIN users u ON u.user_id = w.user_id
        WHERE w.id = ?
        """,
            (int(withdrawal_id),),
    ) as cur:
        return await cur.fetchone()

async def set_withdrawal_status(db: aiosqlite.Connection, withdrawal_id: int, status: str, processed_by: Optional[int] = None) -> None:
    await db.execute(
        """
        UPDATE withdrawals
        SET status = ?,
            processed_at = datetime('now'),
            processed_by = ?
        WHERE id = ?
        """,
        (status, processed_by, int(withdrawal_id)),
    )

async def user_withdrawals(db: aiosqlite.Connection, user_id: int, limit: int = 20):
    async with db.execute(
            """
        SELECT id, amount, method, status, created_at
        FROM withdrawals
        WHERE user_id = ?
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
            (int(user_id), int(limit)),
    ) as cur:
        return await cur.fetchall()

async def balances_audit(db: aiosqlite.Connection, limit: int = 10):
    async with db.execute(
            """
        SELECT
          u.user_id,
          u.username,
          COALESCE(u.balance, 0) AS users_balance,
          COALESCE(SUM(l.delta), 0) AS ledger_sum,
          (COALESCE(u.balance, 0) - COALESCE(SUM(l.delta), 0)) AS diff
        FROM users u
        LEFT JOIN ledger l ON l.user_id = u.user_id
        GROUP BY u.user_id
        HAVING ABS(diff) > 1e-9
        ORDER BY ABS(diff) DESC
        LIMIT ?
        """,
            (int(limit),),
    ) as cur:
        return await cur.fetchall()

async def apply_balance_delta(
        db: aiosqlite.Connection,
        user_id: int,
        delta: float,
        reason: str,
        campaign_key: Optional[str] = None,
        withdrawal_id: Optional[int] = None,
        meta: Optional[str] = None,
) -> None:
    await db.execute(
        """
        INSERT INTO ledger (user_id, delta, reason, campaign_key, withdrawal_id, meta, created_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (int(user_id), float(delta), reason, campaign_key, withdrawal_id, meta),
    )

    await db.execute(
        "UPDATE users SET balance = balance + ? WHERE user_id = ?",
        (float(delta), int(user_id)),
    )

async def apply_balance_debit_if_enough(
        db: aiosqlite.Connection,
        user_id: int,
        amount: float,
        reason: str,
        campaign_key: Optional[str] = None,
        withdrawal_id: Optional[int] = None,
        meta: Optional[str] = None,
) -> bool:
    amount = float(amount)

    cur = await db.execute(
        "UPDATE users SET balance = balance - ? WHERE user_id = ? AND balance >= ?",
        (amount, int(user_id), amount),
    )
    if cur.rowcount != 1:
        return False

    await db.execute(
        """
        INSERT INTO ledger (user_id, delta, reason, campaign_key, withdrawal_id, meta, created_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (int(user_id), -amount, reason, campaign_key, withdrawal_id, meta),
    )
    return True

def stars(value) -> Decimal:
    return Decimal(value).quantize(Decimal("0.00"), rounding=ROUND_DOWN)

def fmt_stars(v):
    return f"{Decimal(v):.2f}"

async def admin_balance_changes(db: aiosqlite.Connection) -> tuple[int, int]:
    query = """
    SELECT
        COALESCE(SUM(CASE WHEN delta > 0 THEN delta END), 0) AS added,
        COALESCE(SUM(CASE WHEN delta < 0 THEN -delta END), 0) AS removed
    FROM ledger
    WHERE reason = 'admin_adjust'
    """

    async with db.execute(query) as cur:
        row = await cur.fetchone()
        added = int(row[0] or 0)
        removed = int(row[1] or 0)

    return added, removed

async def total_withdrawn_amount(db: aiosqlite.Connection) -> int:
    query = """
    SELECT COALESCE(SUM(amount), 0)
    FROM withdrawals
    WHERE status = 'paid'
    """

    async with db.execute(query) as cur:
        row = await cur.fetchone()
        return int(row[0] or 0)

async def pending_withdrawn_amount(db: aiosqlite.Connection) -> int:
    query = """
    SELECT COALESCE(SUM(amount), 0)
    FROM withdrawals
    WHERE status = 'pending'
    """
    async with db.execute(query) as cur:
        row = await cur.fetchone()
        return int(row[0] or 0)

async def ledger_sum_by_reason(db: aiosqlite.Connection, reason: str) -> float:
    query = """
    SELECT COALESCE(SUM(delta), 0)
    FROM ledger
    WHERE reason = ?
    """
    async with db.execute(query, (reason,)) as cur:
        row = await cur.fetchone()
        return float(row[0] or 0)


async def cleanup_abuse_events(db: aiosqlite.Connection) -> None:
    await db.execute("""
        DELETE FROM abuse_events
        WHERE datetime(created_at) < datetime('now', '-1 day')
    """)


async def log_abuse_event(db, user_id: int, action: str, amount: float = 0):
    await cleanup_abuse_events(db)

    await db.execute(
        """
        INSERT INTO abuse_events (user_id, action, amount)
        VALUES (?, ?, ?)
        """,
        (int(user_id), action, float(amount)),
    )


async def count_recent_abuse_events(
        db: aiosqlite.Connection,
        user_id: int,
        action: str,
        minutes: int,
) -> int:
    async with db.execute(
            """
        SELECT COUNT(*)
        FROM abuse_events
        WHERE user_id = ?
          AND action = ?
          AND datetime(created_at) >= datetime('now', ?)
        """,
            (int(user_id), action, f"-{int(minutes)} minutes"),
    ) as cur:
        row = await cur.fetchone()
        return int(row[0] or 0)


async def sum_recent_abuse_amount(
        db: aiosqlite.Connection,
        user_id: int,
        action: str,
        hours: int,
) -> float:
    async with db.execute(
            """
        SELECT COALESCE(SUM(amount), 0)
        FROM abuse_events
        WHERE user_id = ?
          AND action = ?
          AND datetime(created_at) >= datetime('now', ?)
        """,
            (int(user_id), action, f"-{int(hours)} hours"),
    ) as cur:
        row = await cur.fetchone()
        return float(row[0] or 0.0)


async def has_pending_withdrawal(db: aiosqlite.Connection, user_id: int) -> bool:
    async with db.execute(
            """
        SELECT 1
        FROM withdrawals
        WHERE user_id = ?
          AND status = 'pending'
        LIMIT 1
        """,
            (int(user_id),),
    ) as cur:
        return await cur.fetchone() is not None


async def user_created_hours_ago(db: aiosqlite.Connection, user_id: int) -> float:
    async with db.execute(
            """
        SELECT COALESCE((julianday('now') - julianday(created_at)) * 24.0, 0)
        FROM users
        WHERE user_id = ?
        """,
            (int(user_id),),
    ) as cur:
        row = await cur.fetchone()
        return float(row[0] or 0.0)

async def wallet_used_by_another_user(
        db: aiosqlite.Connection,
        user_id: int,
        details: str,
) -> bool:
    async with db.execute(
            """
        SELECT 1
        FROM withdrawals
        WHERE method = 'ton'
          AND details = ?
          AND user_id != ?
        LIMIT 1
        """,
            (details.strip(), int(user_id)),
    ) as cur:
        return await cur.fetchone() is not None

async def wallet_users(db, details: str) -> list[str]:
    async with db.execute(
            """
        SELECT DISTINCT w.user_id, u.username
        FROM withdrawals w
        LEFT JOIN users u ON u.user_id = w.user_id
        WHERE w.details = ?
        ORDER BY w.user_id ASC
        """,
            (details.strip(),)
    ) as cur:
        rows = await cur.fetchall()

    result = []
    for user_id, username in rows:
        if username:
            result.append(f"@{username}")
        else:
            result.append(f"user_id={user_id}")

    return result

async def mark_user_suspicious(db, user_id: int, reason: str):
    row = await db.execute_fetchone(
        "SELECT is_suspicious, suspicious_reason FROM users WHERE user_id = ?",
        (user_id,),
    )
    if not row:
        return

    if row["is_suspicious"]:
        old_reason = row["suspicious_reason"] or ""
        if reason and reason not in old_reason:
            new_reason = f"{old_reason}; {reason}" if old_reason else reason
        else:
            new_reason = old_reason
    else:
        new_reason = reason

    await db.execute(
        """
        UPDATE users
        SET is_suspicious = 1,
            suspicious_reason = ?
        WHERE user_id = ?
        """,
        (new_reason, user_id),
    )
    await db.commit()

async def clear_user_suspicious(db, user_id: int):
    await db.execute(
        """
        UPDATE users
        SET is_suspicious = 0,
            suspicious_reason = NULL
        WHERE user_id = ?
        """,
        (user_id,),
    )
    await db.commit()

async def get_user_earnings_breakdown(db, user_id: int):
    cursor = await db.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN reason = 'task_bonus' THEN delta ELSE 0 END), 0) AS task_bonus,
            COALESCE(SUM(CASE WHEN reason = 'contest_bonus' THEN delta ELSE 0 END), 0) AS contest_bonus,
            COALESCE(SUM(CASE WHEN reason = 'daily_bonus' THEN delta ELSE 0 END), 0) AS daily_bonus,
            COALESCE(SUM(CASE WHEN reason = 'referral_bonus' THEN delta ELSE 0 END), 0) AS referral_bonus,
            COALESCE(SUM(CASE WHEN reason = 'admin_adjust' THEN delta ELSE 0 END), 0) AS admin_adjust,
            COALESCE(SUM(CASE
                WHEN reason NOT IN ('withdraw_hold', 'withdraw_paid', 'withdraw_release')
                THEN delta ELSE 0 END), 0) AS total_earned
        FROM ledger
        WHERE user_id = ?
        """,
        (user_id,),
    )
    row = await cursor.fetchone()

    tasks = row["task_bonus"] or 0
    contests = row["contest_bonus"] or 0
    daily_checkin = row["daily_bonus"] or 0
    referrals = row["referral_bonus"] or 0
    admin_adjust = row["admin_adjust"] or 0
    total = row["total_earned"] or 0

    def pct(value: float, total_value: float) -> int:
        if total_value == 0:
            return 0
        return round(value * 100 / total_value)

    return {
        "total": total,
        "tasks": tasks,
        "tasks_pct": pct(tasks, total),
        "contests": contests,
        "contests_pct": pct(contests, total),
        "daily_checkin": daily_checkin,
        "daily_checkin_pct": pct(daily_checkin, total),
        "referrals": referrals,
        "referrals_pct": pct(referrals, total),
        "admin_adjust": admin_adjust,
        "admin_adjust_pct": pct(admin_adjust, total),
    }

async def build_user_details_text(db, user_id: int) -> str:
    cursor = await db.execute(
        """
        SELECT user_id, username, balance, is_suspicious, suspicious_reason
        FROM users
        WHERE user_id = ?
        """,
        (user_id,),
    )
    user = await cursor.fetchone()

    if not user:
        return "❌ Пользователь не найден."

    if user["is_suspicious"]:
        suspicious_block = (
            f"⚠️ Подозрительный\n"
            f"Причина: {user['suspicious_reason'] or '-'}"
        )
    else:
        suspicious_block = "✅ Не подозрительный"

    return (
        f"👤 Пользователь: {user['user_id']}\n"
        f"Username: @{user['username'] or '-'}\n"
        f"Баланс: {fmt_stars(user['balance'])}⭐\n\n"
        f"{suspicious_block}"
    )

async def build_user_stats_text(db, user_id: int) -> str:
    stats = await get_user_earnings_breakdown(db, user_id)

    return (
        f"⭐ Всего заработано: {fmt_stars(stats['total'])}⭐\n"
        f"{fmt_stars(stats['tasks'])} ({stats['tasks_pct']}%) — задания\n"
        f"{fmt_stars(stats['contests'])} ({stats['contests_pct']}%) — конкурсы\n"
        f"{fmt_stars(stats['daily_checkin'])} ({stats['daily_checkin_pct']}%) — дейли чекин\n"
        f"{fmt_stars(stats['referrals'])} ({stats['referrals_pct']}%) — рефералы\n"
        f"{fmt_stars(stats['admin_adjust'])} ({stats['admin_adjust_pct']}%) — начисления от админа"
    )

async def mark_withdraw_fee_refunded(db, withdrawal_id: int):
    await db.execute(
        """
        UPDATE withdrawals
        SET fee_refunded = 1
        WHERE id = ?
        """,
        (withdrawal_id,),
    )
    await db.commit()

async def list_recent_fee_payments(db, limit: int = 10):
    cur = await db.execute(
        """
        SELECT
            w.id AS withdrawal_id,
            w.user_id,
            u.username AS username,
            w.fee_xtr,
            w.fee_paid,
            w.fee_refunded,
            w.fee_telegram_charge_id,
            w.created_at
        FROM withdrawals w
        LEFT JOIN users u ON u.user_id = w.user_id
        WHERE w.fee_paid = 1
          AND w.fee_telegram_charge_id IS NOT NULL
          AND w.fee_telegram_charge_id != ''
        ORDER BY w.id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = await cur.fetchall()
    await cur.close()
    return rows


async def find_withdraw_by_fee_charge_id(db, charge_id: str):
    cur = await db.execute(
        """
        SELECT
            w.id AS withdrawal_id,
            w.user_id,
            w.fee_xtr,
            w.fee_paid,
            w.fee_refunded,
            w.fee_telegram_charge_id,
            w.created_at
        FROM withdrawals w
        WHERE w.fee_telegram_charge_id = ?
        LIMIT 1
        """,
        (charge_id,),
    )
    row = await cur.fetchone()
    await cur.close()
    return row


async def xtr_ledger_add(
        db: aiosqlite.Connection,
        user_id: int,
        delta_xtr: int,
        reason: str,
        withdrawal_id: Optional[int] = None,
        telegram_payment_charge_id: Optional[str] = None,
        invoice_payload: Optional[str] = None,
        meta: Optional[str] = None,
) -> None:
    await db.execute(
        """
        INSERT INTO xtr_ledger (
            user_id,
            withdrawal_id,
            delta_xtr,
            reason,
            telegram_payment_charge_id,
            invoice_payload,
            meta,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            int(user_id),
            int(withdrawal_id) if withdrawal_id is not None else None,
            int(delta_xtr),
            reason,
            telegram_payment_charge_id,
            invoice_payload,
            meta,
        ),
    )


async def xtr_ledger_sum(db: aiosqlite.Connection) -> int:
    async with db.execute(
            "SELECT COALESCE(SUM(delta_xtr), 0) AS s FROM xtr_ledger"
    ) as cur:
        row = await cur.fetchone()
        return int(row["s"] or 0)


async def xtr_ledger_sum_by_reason(db: aiosqlite.Connection, reason: str) -> int:
    async with db.execute(
            """
        SELECT COALESCE(SUM(delta_xtr), 0) AS s
        FROM xtr_ledger
        WHERE reason = ?
        """,
            (reason,),
    ) as cur:
        row = await cur.fetchone()
        return int(row["s"] or 0)
