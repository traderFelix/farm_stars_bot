import html
import logging
import traceback

from aiogram import Router, Bot
from aiogram.types import ErrorEvent, CallbackQuery, Message

from config import ADMIN_IDS

router = Router()
logger = logging.getLogger(__name__)


async def _send_admin_trace(bot: Bot, text: str):
    # Telegram лимит ~4096 символов, режем на части
    chunk_size = 3500
    for i in range(0, len(text), chunk_size):
        for admin_id in ADMIN_IDS:
            await bot.send_message(admin_id, text[i:i + chunk_size])


@router.error()
async def global_error_handler(event: ErrorEvent, bot: Bot):
    exc = event.exception
    logger.exception("Unhandled exception", exc_info=exc)

    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))

    update_preview = html.escape(str(event.update))[:1500]
    trace_text = html.escape(tb)[:12000]

    admin_text = (
        "🚨 Ошибка в боте\n\n"
        f"<b>Exception:</b>\n<pre>{html.escape(repr(exc))}</pre>\n\n"
        f"<b>Update:</b>\n<pre>{update_preview}</pre>\n\n"
        f"<b>Traceback:</b>\n<pre>{trace_text}</pre>"
    )

    try:
        await _send_admin_trace(bot, admin_text)
    except Exception:
        logger.exception("Failed to send error to admin")

    # Ничего технического пользователю не показываем
    try:
        if isinstance(event.update.callback_query, CallbackQuery):
            await event.update.callback_query.answer(
                "❌ Произошла ошибка. Уже чиним.",
                show_alert=True
            )
        elif isinstance(event.update.message, Message):
            await event.update.message.answer("❌ Произошла ошибка. Попробуй ещё раз.")
    except Exception:
        logger.exception("Failed to notify user about error")

    return True