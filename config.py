import os

TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_LINK = os.getenv("CHANNEL_LINK", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "")
ADMIN_IDS = set(map(int, os.getenv("ADMIN_IDS", "").split(",")))\
    if os.getenv("ADMIN_IDS")\
    else set()

DB_PATH = "bot.db"
MIN_WITHDRAW = 100.0
MIN_WITHDRAW_PERCENTAGE = 0.25
VIEW_POST_SECONDS = 3
VIEW_POST_REWARD = 0.01
VIEW_POST_REQUIRED_VIEWS = 0