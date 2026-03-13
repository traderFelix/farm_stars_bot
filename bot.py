import asyncio, logging

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config import TOKEN
from db import open_db, close_db, init_db
from handlers import user_router, admin_router, errors_router
from middlewares.db import DbMiddleware

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)

logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)

async def main():
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN не задан.")

    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    db = await open_db()

    try:
        await init_db(db)

        dp.update.middleware(DbMiddleware(db))

        dp.include_router(user_router)
        dp.include_router(admin_router)
        dp.include_router(errors_router)

        await dp.start_polling(bot)

    finally:
        await close_db(db)


if __name__ == "__main__":
    asyncio.run(main())