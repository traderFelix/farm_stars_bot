import asyncio
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config import TOKEN
from db import init_db
from handlers import user_router, admin_router

async def main():
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN не задан. Установи: export BOT_TOKEN='...'")

    init_db()

    bot = Bot(token=TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(user_router)
    dp.include_router(admin_router)

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
