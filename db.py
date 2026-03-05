import sqlite3

from typing import Optional, List

conn = sqlite3.connect("bot.db")
cursor = conn.cursor()

def init_db():
    cursor.executescript("""
    CREATE TABLE IF NOT EXISTS users (
      user_id INTEGER PRIMARY KEY,
      username TEXT,
      tg_first_name TEXT,
      tg_last_name TEXT,
      balance REAL DEFAULT 0,
      is_banned INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      last_seen_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS campaigns (
      campaign_key TEXT PRIMARY KEY,
      title TEXT NOT NULL,
      reward_amount REAL NOT NULL,
      status TEXT DEFAULT 'draft',             -- draft | active | ended
      description TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      starts_at TEXT,
      ends_at TEXT
    );

    CREATE TABLE IF NOT EXISTS claims (
      user_id INTEGER NOT NULL,
      campaign_key TEXT NOT NULL,
      amount REAL NOT NULL,
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
      delta REAL NOT NULL,
      reason TEXT NOT NULL,
      campaign_key TEXT,
      withdrawal_id INTEGER,
      meta TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(user_id)
    );
    
    CREATE TABLE IF NOT EXISTS withdrawals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      amount REAL NOT NULL,
      method TEXT NOT NULL,            -- 'ton' | 'stars'
      details TEXT,                    -- wallet address for TON
      status TEXT NOT NULL DEFAULT 'pending',  -- pending|paid|rejected
      created_at TEXT DEFAULT (datetime('now')),
      processed_at TEXT,
      processed_by INTEGER,
      FOREIGN KEY (user_id) REFERENCES users(user_id)
    );

    CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at);
    CREATE INDEX IF NOT EXISTS idx_users_last_seen_at ON users(last_seen_at);
    CREATE INDEX IF NOT EXISTS idx_claims_campaign_key ON claims(campaign_key);
    CREATE INDEX IF NOT EXISTS idx_winners_campaign_key ON campaign_winners(campaign_key);
    CREATE INDEX IF NOT EXISTS idx_ledger_withdrawal ON ledger(withdrawal_id);
    CREATE INDEX IF NOT EXISTS idx_withdrawals_user_created ON withdrawals(user_id, created_at);
    CREATE INDEX IF NOT EXISTS idx_withdrawals_status_created ON withdrawals(status, created_at);
    """)

    conn.commit()


# ---------- Users ----------

def register_user(user_id: int, username: Optional[str], first_name: Optional[str] = None, last_name: Optional[str] = None):
    u = (username or "").strip().lstrip("@") or None
    fn = (first_name or "").strip() or None
    ln = (last_name or "").strip() or None

    cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    exists = cursor.fetchone() is not None

    if not exists:
        cursor.execute(
            """
            INSERT INTO users (user_id, username, tg_first_name, tg_last_name, balance, created_at, last_seen_at)
            VALUES (?, ?, ?, ?, 0, datetime('now'), datetime('now'))
            """,
            (user_id, u, fn, ln),
        )
        conn.commit()
        return

    cursor.execute(
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
    conn.commit()


def get_balance(user_id: int) -> float:
    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    return float(row[0]) if row else 0.0


def add_balance(user_id: int, amount: float):
    cursor.execute(
        "UPDATE users SET balance = balance + ? WHERE user_id = ?",
        (float(amount), user_id),
    )


def total_balances() -> float:
    cursor.execute("SELECT COALESCE(SUM(balance), 0) FROM users")
    row = cursor.fetchone()
    return float(row[0]) if row and row[0] is not None else 0.0


def top_users_by_balance(limit: int = 10):
    cursor.execute(
        """
        SELECT username, balance
        FROM users
        ORDER BY balance DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    return cursor.fetchall()


# ---------- Campaigns ----------

def upsert_campaign(campaign_key: str, title: str, reward_amount: float, status: str = "draft", description: Optional[str] = None):
    cursor.execute(
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
    conn.commit()


def set_campaign_status(campaign_key: str, status: str):
    cursor.execute(
        "UPDATE campaigns SET status = ? WHERE campaign_key = ?",
        (status, campaign_key),
    )
    conn.commit()


def delete_campaign(campaign_key: str):
    cursor.execute("DELETE FROM claims WHERE campaign_key = ?", (campaign_key,))
    cursor.execute("DELETE FROM campaign_winners WHERE campaign_key = ?", (campaign_key,))
    cursor.execute("DELETE FROM campaigns WHERE campaign_key = ?", (campaign_key,))
    conn.commit()


def get_campaign(campaign_key: str):
    cursor.execute(
        "SELECT campaign_key, title, reward_amount, status FROM campaigns WHERE campaign_key = ?",
        (campaign_key,),
    )
    return cursor.fetchone()


def list_campaigns():
    cursor.execute(
        """
        SELECT campaign_key, reward_amount, status, created_at
        FROM campaigns
        ORDER BY datetime(created_at) DESC
        """
    )
    return cursor.fetchall()


def list_campaigns_latest(limit: int = 5):
    cursor.execute(
        """
        SELECT campaign_key, reward_amount, status, created_at
        FROM campaigns
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    return cursor.fetchall()


def list_active_campaigns():
    cursor.execute(
        """
        SELECT campaign_key, title, reward_amount
        FROM campaigns
        WHERE status = 'active'
        ORDER BY datetime(created_at) DESC
        """
    )
    return cursor.fetchall()


def campaigns_status_counts():
    cursor.execute("SELECT status, COUNT(*) FROM campaigns GROUP BY status")
    rows = cursor.fetchall()

    counts = {"active": 0, "ended": 0, "draft": 0}
    for st, cnt in rows:
        counts[str(st)] = int(cnt)

    return counts["active"], counts["ended"], counts["draft"]


# ---------- Winners ----------

def add_winners(campaign_key: str, usernames: List[str]) -> int:
    count = 0
    for u in usernames:
        u = (u or "").strip().lstrip("@")
        if not u:
            continue
        cursor.execute(
            "INSERT OR IGNORE INTO campaign_winners (campaign_key, username) VALUES (?, ?)",
            (campaign_key, u),
        )
        count += 1
    conn.commit()
    return count


def list_winners(campaign_key: str) -> List[str]:
    cursor.execute(
        "SELECT username FROM campaign_winners WHERE campaign_key = ? ORDER BY added_at ASC",
        (campaign_key,),
    )
    return [r[0] for r in cursor.fetchall()]


def winners_count(campaign_key: str) -> int:
    cursor.execute("SELECT COUNT(*) FROM campaign_winners WHERE campaign_key = ?", (campaign_key,))
    return int(cursor.fetchone()[0])


def attach_winner_user_id(campaign_key: str, username: str, user_id: int) -> None:
    u = (username or "").strip().lstrip("@")
    if not u:
        return
    cursor.execute(
        """
        UPDATE campaign_winners
        SET user_id = ?
        WHERE campaign_key = ?
          AND username = ?
          AND user_id IS NULL
        """,
        (int(user_id), campaign_key, u),
    )
    conn.commit()


def is_winner(campaign_key: str, user_id: int, username: Optional[str]) -> bool:
    cursor.execute(
        "SELECT 1 FROM campaign_winners WHERE campaign_key = ? AND user_id = ? LIMIT 1",
        (campaign_key, int(user_id)),
    )
    if cursor.fetchone() is not None:
        return True

    u = (username or "").strip().lstrip("@")
    if not u:
        return False

    cursor.execute(
        "SELECT 1 FROM campaign_winners WHERE campaign_key = ? AND username = ? LIMIT 1",
        (campaign_key, u),
    )
    return cursor.fetchone() is not None


def delete_winner_if_not_claimed(campaign_key: str, username: str):
    u = (username or "").strip().lstrip("@")
    if not u:
        return False, "Пустой username"

    cursor.execute(
        "SELECT user_id FROM campaign_winners WHERE campaign_key = ? AND username = ?",
        (campaign_key, u),
    )
    row = cursor.fetchone()
    if row is None:
        return False, "Этого username нет в списке победителей"

    winner_user_id = row[0]

    user_id_by_username = None
    if winner_user_id is None:
        cursor.execute("SELECT user_id FROM users WHERE username = ? LIMIT 1", (u,))
        r2 = cursor.fetchone()
        if r2:
            user_id_by_username = r2[0]

    cursor.execute(
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
    )
    if cursor.fetchone() is not None:
        return False, "Нельзя удалить: этот победитель уже заклеймил"

    cursor.execute(
        "DELETE FROM campaign_winners WHERE campaign_key = ? AND username = ?",
        (campaign_key, u),
    )
    conn.commit()
    return True, "Удалено"


# ---------- Claims ----------

def has_claim(user_id: int, campaign_key: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM claims WHERE user_id = ? AND campaign_key = ?",
        (int(user_id), campaign_key),
    )
    return cursor.fetchone() is not None


def add_claim(user_id: int, campaign_key: str, amount: float) -> None:
    cursor.execute(
        "INSERT INTO claims (user_id, campaign_key, amount) VALUES (?, ?, ?)",
        (int(user_id), campaign_key, float(amount)),
    )


def claimed_usernames(campaign_key: str) -> List[str]:
    cursor.execute(
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
    )
    return [r[0] for r in cursor.fetchall()]


def campaign_stats(campaign_key: str):
    cursor.execute(
        "SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM claims WHERE campaign_key = ?",
        (campaign_key,),
    )
    claims_count, total_paid = cursor.fetchone()
    claims_count = int(claims_count or 0)
    total_paid = float(total_paid or 0.0)

    cursor.execute("SELECT COUNT(*) FROM campaign_winners WHERE campaign_key = ?", (campaign_key,))
    winners_cnt = int(cursor.fetchone()[0])

    return claims_count, winners_cnt, total_paid


def global_claims_stats():
    cursor.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM claims")
    cnt, total = cursor.fetchone()
    return int(cnt or 0), float(total or 0.0)


# ---------- Totals for admin dashboard ----------

def total_assigned_amount() -> float:
    cursor.execute(
        """
        SELECT COALESCE(SUM(c.reward_amount), 0)
        FROM campaign_winners w
        JOIN campaigns c ON c.campaign_key = w.campaign_key
        """
    )
    row = cursor.fetchone()
    return float(row[0]) if row and row[0] is not None else 0.0


def unclaimed_total_amount() -> float:
    cursor.execute(
        """
        SELECT COALESCE(SUM(c.reward_amount), 0)
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
    )
    row = cursor.fetchone()
    return float(row[0]) if row and row[0] is not None else 0.0

def users_total_count() -> int:
    cursor.execute("SELECT COUNT(*) FROM users")
    return int(cursor.fetchone()[0])


def users_new_since_hours(hours: int) -> int:
    cursor.execute(
        "SELECT COUNT(*) FROM users WHERE created_at >= datetime('now', ?)",
        (f"-{int(hours)} hours",),
    )
    return int(cursor.fetchone()[0])


def users_new_since_days(days: int) -> int:
    cursor.execute(
        "SELECT COUNT(*) FROM users WHERE created_at >= datetime('now', ?)",
        (f"-{int(days)} days",),
    )
    return int(cursor.fetchone()[0])


def users_active_since_days(days: int) -> int:
    cursor.execute(
        "SELECT COUNT(*) FROM users WHERE last_seen_at >= datetime('now', ?)",
        (f"-{int(days)} days",),
    )
    return int(cursor.fetchone()[0])


def users_growth_by_day(days: int = 30):
    cursor.execute(
        """
        SELECT date(created_at) AS d, COUNT(*) AS cnt
        FROM users
        WHERE created_at >= datetime('now', ?)
        GROUP BY d
        ORDER BY d ASC
        """,
        (f"-{int(days)} days",),
    )
    return [(row[0], int(row[1])) for row in cursor.fetchall()]

def ledger_add(
        user_id: int,
        delta: float,
        reason: str,
        campaign_key: Optional[str] = None,
        withdrawal_id: Optional[int] = None,
        meta: Optional[str] = None,
):
    cursor.execute(
        """
        INSERT INTO ledger (user_id, delta, reason, campaign_key, withdrawal_id, meta, created_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (int(user_id), float(delta), reason, campaign_key, withdrawal_id, meta),
    )


def ledger_last(user_id: int, limit: int = 20):
    cursor.execute(
        """
        SELECT created_at, delta, reason, campaign_key, meta
        FROM ledger
        WHERE user_id = ?
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    )
    return cursor.fetchall()


def ledger_sum(user_id: int) -> float:
    cursor.execute("SELECT COALESCE(SUM(delta), 0) FROM ledger WHERE user_id = ?", (int(user_id),))
    return float(cursor.fetchone()[0])

def ledger_user_history(user_id: int, limit: int = 20):
    cursor.execute(
        """
        SELECT created_at, delta, reason, campaign_key
        FROM ledger
        WHERE user_id = ?
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    )
    return cursor.fetchall()

def create_withdrawal(user_id: int, amount: float, method: str, details: Optional[str] = None) -> int:
    cursor.execute(
        """
        INSERT INTO withdrawals (user_id, amount, method, details, status)
        VALUES (?, ?, ?, ?, 'pending')
        """,
        (int(user_id), float(amount), method, details),
    )
    return int(cursor.lastrowid)


def list_withdrawals(status: str = "pending", limit: int = 20):
    cursor.execute(
        """
        SELECT w.id, w.user_id, u.username, w.amount, w.method, w.details, w.status, w.created_at
        FROM withdrawals w
        LEFT JOIN users u ON u.user_id = w.user_id
        WHERE w.status = ?
        ORDER BY datetime(w.created_at) DESC
        LIMIT ?
        """,
        (status, int(limit)),
    )
    return cursor.fetchall()


def get_withdrawal(withdrawal_id: int):
    cursor.execute(
        """
        SELECT w.id, w.user_id, u.username, w.amount, w.method, w.details, w.status, w.created_at
        FROM withdrawals w
        LEFT JOIN users u ON u.user_id = w.user_id
        WHERE w.id = ?
        """,
        (int(withdrawal_id),),
    )
    return cursor.fetchone()


def set_withdrawal_status(withdrawal_id: int, status: str, processed_by: Optional[int] = None):
    cursor.execute(
        """
        UPDATE withdrawals
        SET status = ?,
            processed_at = datetime('now'),
            processed_by = ?
        WHERE id = ?
        """,
        (status, processed_by, int(withdrawal_id)),
    )

def user_withdrawals(user_id: int, limit: int = 20):
    cursor.execute(
        """
        SELECT id, amount, method, status, created_at
        FROM withdrawals
        WHERE user_id = ?
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
        (int(user_id), int(limit)),
    )
    return cursor.fetchall()

def balances_audit(limit: int = 10):
    cursor.execute(
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
    )
    return cursor.fetchall()
