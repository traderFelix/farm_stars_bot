import asyncio
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config import TOKEN
from db import open_db, close_db, init_db
from handlers import user_router, admin_router
from middlewares.db import DbMiddleware


async def on_startup(bot, dispatcher):
    db = await open_db()
    await init_db(db)
    dispatcher.update.middleware(DbMiddleware(db))


async def on_shutdown(bot: Bot, dispatcher: Dispatcher):
    db = dispatcher.get("db")
    if db:
        await close_db(db)


async def main():
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN не задан.")

    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(user_router)
    dp.include_router(admin_router)

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())