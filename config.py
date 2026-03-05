import os

TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "")
ADMIN_IDS = set(map(int, os.getenv("ADMIN_IDS", "").split(",")))\
    if os.getenv("ADMIN_IDS")\
    else set()

MIN_WITHDRAW = 100.0