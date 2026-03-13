import html
import logging
import traceback

from aiogram import Router, Bot
from aiogram.types import ErrorEvent
from aiogram.exceptions import TelegramForbiddenError

from config import ADMIN_IDS

router = Router()
logger = logging.getLogger(__name__)


async def _send_admin_trace(bot: Bot, text: str):
    # Telegram лимит ~4096 символов, режем на части
    chunk_size = 3500
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i + chunk_size]
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, chunk)
            except Exception:
                logger.exception("Failed to send error chunk to admin %s", admin_id)


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

    # Если бот заблокирован пользователем — просто молча выходим
    if isinstance(exc, TelegramForbiddenError):
        return True

    try:
        if event.update.callback_query:
            await event.update.callback_query.answer(
                "❌ Произошла ошибка. Уже чиним.",
                show_alert=True
            )
        elif event.update.message:
            await event.update.message.answer("❌ Произошла ошибка. Попробуй ещё раз.")
    except TelegramForbiddenError:
        pass
    except Exception:
        logger.exception("Failed to notify user about error")

    return True