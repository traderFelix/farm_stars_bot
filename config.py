import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(), override=False)

def _parse_ids(env_name: str) -> set[int]:
    raw = (os.getenv(env_name) or "").strip()
    if not raw:
        return set()

    result: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        result.add(int(part))
    return result

OWNER_ID = _parse_ids("OWNER_ID")
ADMIN_IDS = _parse_ids("ADMIN_IDS")

TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "")

ROLE_USER = 0
ROLE_CLIENT = 3
ROLE_PARTNER = 6
ROLE_ADMIN = 9
ROLE_OWNER = 10

DB_PATH = "bot.db"
MIN_WITHDRAW = 100.0
MIN_WITHDRAW_PERCENT = 0.25
LEDGER_PAGE_SIZE = 20
REFERRAL_PERCENT = 0.10