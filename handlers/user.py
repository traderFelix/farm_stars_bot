import asyncio, logging

from typing import Optional

from aiogram.filters import CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, PreCheckoutQuery, LabeledPrice
)

from config import CHANNEL_ID, ADMIN_IDS, MIN_WITHDRAW, MIN_WITHDRAW_PERCENT

from db import (
    sum_recent_abuse_amount, has_pending_withdrawal, user_created_hours_ago, get_user_earnings_breakdown,
    register_user, get_balance, create_withdrawal, user_withdrawals, apply_balance_debit_if_enough,
    claim_reward, list_active_campaigns, log_abuse_event, count_recent_abuse_events, tx, fmt_stars,
    wallet_used_by_another_user, wallet_users, ensure_user_registered, xtr_ledger_add, apply_balance_delta,
    increment_task_post_views, count_available_task_posts_for_user, get_next_task_post_for_user, bind_referrer,
    get_specific_task_post_for_user, add_task_post_view, allocate_task_post_from_channel_post, get_referrals_count,
)

from keyboards import (
    subscribe_keyboard, main_menu, tasks_menu, bottom_menu_kb, withdraw_stars_amount_kb, task_after_view_kb,
    withdraw_method_kb, withdraw_menu_kb, withdraw_back_kb, referrals_kb
)

from states import WithdrawCreate

router = Router()

logger = logging.getLogger(__name__)

WITHDRAW_TEXT = f"""
💰 <b>Вывод и обмен звёзд</b>

🔷 Минимальная сумма вывода и обмена <b>{MIN_WITHDRAW}⭐</b>
🔷 Конвертация звёзд в TON производится по курсу на сайте <b>Fragment</b>
🔷 Для вывода необходимо, чтобы минимум <b>{MIN_WITHDRAW_PERCENT * 100:.0f}%</b> звезд на балансе были добыты путем выполнения заданий

<blockquote>
<b>Первый вывод бесплатный 🔥</b>

Последующие выводы:
▪️ 100⭐ — комиссия <b>5 Telegram Stars</b>
▪️ 200⭐ — комиссия <b>3 Telegram Stars</b>
▪️ 500⭐ — <b>без комиссии</b>

💡 Комиссия списывается <b>только с баланса Telegram Stars</b>, а не с игрового баланса звёзд
</blockquote>

Выберите нужный вариант ниже! 👇
"""

def menu_text(balance: float) -> str:
    return "Чтобы получить больше ⭐️, выполняйте задания\n\n" + f"Баланс: {fmt_stars(balance)}⭐️"


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

@router.channel_post()
async def ingest_task_channel_post(message: Message, db):
    has_content = bool(
        message.text
        or message.caption
        or message.photo
        or message.video
        or message.animation
        or message.document
    )
    if not has_content:
        return

    async with tx(db, immediate=True):
        await allocate_task_post_from_channel_post(
            db=db,
            chat_id=str(message.chat.id),
            channel_post_id=int(message.message_id),
            title=message.chat.title,
            reward=0.01,
        )

@router.message(StateFilter("*"), F.text == "🏠 Главное меню")
async def open_main_menu_from_bottom_button(message: Message, state: FSMContext, db):
    await state.clear()

    async with tx(db, immediate=False):
        await ensure_user_registered(message, db)

    user_id = message.from_user.id
    balance = await get_balance(db, user_id)

    await message.answer(
        menu_text(balance),
        reply_markup=main_menu(is_admin(user_id)),
    )


@router.message(CommandStart())
async def start(message: Message, bot: Bot, db):
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name

    start_arg = None
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) > 1:
        start_arg = parts[1].strip()

    logger.info("START user_id=%s text=%r start_arg=%r", user_id, message.text, start_arg)

    async with tx(db, immediate=False):
        await register_user(db, user_id, username, first_name, last_name)

        if start_arg and start_arg.isdigit():
            try:
                bound = await bind_referrer(db, user_id, int(start_arg))
                logger.info(
                    "bind_referrer user_id=%s referrer_id=%s bound=%s",
                    user_id, int(start_arg), bound
                )
            except Exception:
                logger.exception(
                    "Failed to bind referrer user_id=%s referrer_id=%s",
                    user_id, start_arg
                )

    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
    except Exception:
        try:
            await message.answer("Ошибка проверки канала.")
        except TelegramForbiddenError:
            logger.warning("User %s blocked bot during channel check error reply", user_id)
        return

    try:
        if member.status in ("member", "administrator", "creator"):
            balance = await get_balance(db, user_id)

            await message.answer(
                "Нажми кнопку снизу, чтобы открыть меню 👇",
                reply_markup=bottom_menu_kb()
            )

            await message.answer(
                menu_text(balance),
                reply_markup=main_menu(is_admin(user_id))
            )
        else:
            await message.answer(
                "Чтобы продолжить, подпишись на канал 👇",
                reply_markup=subscribe_keyboard()
            )
    except TelegramForbiddenError:
        logger.warning("User %s blocked bot", user_id)


@router.callback_query(F.data == "check_sub")
async def check_subscription(callback: CallbackQuery, bot: Bot, db):
    user_id = callback.from_user.id

    async with tx(db, immediate=False):
        await ensure_user_registered(callback, db)

    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
    except Exception:
        await callback.answer("Ошибка проверки канала", show_alert=True)
        return

    if member.status in ("member", "administrator", "creator"):
        balance = await get_balance(db, user_id)

        await callback.message.answer(
            "Нажми кнопку снизу, чтобы открыть меню 👇",
            reply_markup=bottom_menu_kb()
        )

        await callback.message.edit_text(
            menu_text(balance),
            reply_markup=main_menu(is_admin(user_id))
        )
    else:
        await callback.answer("❌ Ты ещё не подписан!", show_alert=True)


@router.callback_query(F.data == "tasks")
async def show_tasks(callback: CallbackQuery, db):
    await callback.answer()

    user_id = callback.from_user.id
    balance = await get_balance(db, user_id)
    available = await count_available_task_posts_for_user(db, user_id)

    await callback.message.edit_text(
        "📋 Задания\n\n"
        "👁 Просмотр постов из каналов\n"
        "За каждый просмотр начисляется награда.\n"
        f"Доступно постов: {available}\n\n"
        f"Баланс: {fmt_stars(balance)}⭐️",
        reply_markup=tasks_menu()
    )


@router.callback_query(F.data == "task:view_post")
async def task_view_post(callback: CallbackQuery, bot: Bot, db):
    user_id = callback.from_user.id

    async with tx(db, immediate=False):
        await ensure_user_registered(callback, db)

    row = await get_next_task_post_for_user(db, user_id)
    if not row:
        await callback.answer("❌ Доступных постов пока нет.", show_alert=True)
        return

    view_seconds = int(row["view_seconds"])

    recent_clicks = await count_recent_abuse_events(db, user_id, "task_view_click", 1)
    if recent_clicks >= 60 / view_seconds:
        await callback.answer("⏳ Слишком часто. Попробуй через минуту.", show_alert=True)
        return

    await log_abuse_event(db, user_id, "task_view_click")

    task_post_id = int(row["id"])
    from_chat_id = row["chat_id"]
    channel_post_id = int(row["channel_post_id"])
    reward = float(row["reward"] or 0)

    await callback.answer("Показываю пост...")

    try:
        await callback.message.delete()
    except Exception:
        pass

    try:
        sent = await bot.forward_message(
            chat_id=user_id,
            from_chat_id=from_chat_id,
            message_id=channel_post_id,
        )
    except TelegramBadRequest:
        await bot.send_message(
            chat_id=user_id,
            text=(
                "❌ Не удалось показать пост.\n"
                "Проверь, что бот есть в канале и видит этот пост."
            )
        )
        return

    await asyncio.sleep(view_seconds)

    try:
        await bot.delete_message(chat_id=user_id, message_id=sent.message_id)
    except Exception:
        pass

    async with tx(db, immediate=True):
        current_row = await get_specific_task_post_for_user(db, user_id, task_post_id)
        if not current_row:
            await bot.send_message(
                chat_id=user_id,
                text=(
                    "⚠️ Этот пост уже стал недоступен.\n"
                    "Попробуй открыть следующий."
                ),
                reply_markup=task_after_view_kb()
            )
            return

        inserted = await add_task_post_view(
            db=db,
            user_id=user_id,
            task_post_id=task_post_id,
            reward=reward,
        )
        if not inserted:
            await bot.send_message(
                chat_id=user_id,
                text="✅ Этот пост уже был засчитан ранее.",
                reply_markup=task_after_view_kb()
            )
            return

        updated = await increment_task_post_views(db, task_post_id)
        if not updated:
            await bot.send_message(
                chat_id=user_id,
                text="⚠️ Не удалось засчитать просмотр. Попробуй следующий пост.",
                reply_markup=task_after_view_kb()
            )
            return

        await apply_balance_delta(
            db=db,
            user_id=user_id,
            delta=reward,
            reason="view_post_bonus",
            meta=f"task_post:{task_post_id}:channel_post:{channel_post_id}",
        )

    new_balance = await get_balance(db, user_id)
    available = await count_available_task_posts_for_user(db, user_id)

    await bot.send_message(
        chat_id=user_id,
        text=(
            "✅ Просмотр засчитан\n\n"
            f"Начислено: {fmt_stars(reward)}⭐\n"
            f"Осталось доступно постов: {available}\n"
            f"Баланс: {fmt_stars(new_balance)}⭐️"
        ),
        reply_markup=task_after_view_kb()
    )


@router.callback_query(F.data == "back")
async def back_to_main(callback: CallbackQuery, db):
    user_id = callback.from_user.id
    balance = await get_balance(db, user_id)
    await callback.answer()
    await callback.message.edit_text(menu_text(balance), reply_markup=main_menu(is_admin(user_id)))


@router.callback_query(F.data == "claim")
async def claim_menu(callback: CallbackQuery, db):
    campaigns = await list_active_campaigns(db)

    if not campaigns:
        await callback.answer("❌ Сейчас нет активных конкурсов", show_alert=True)
        return

    await callback.answer()

    keyboard = []
    for row in campaigns:
        key, title, amount = row[0], row[1], row[2]
        keyboard.append([
            InlineKeyboardButton(
                text=f"🎁 {title} • {amount}⭐",
                callback_data=f"claim:{key}"
            )
        ])

    keyboard.append([InlineKeyboardButton(text="⬅ Назад", callback_data="back")])

    text = "Выбери конкурс для получения награды:"
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    if callback.message.text == text:
        await callback.message.edit_reply_markup(reply_markup=markup)
        return

    await callback.message.edit_text(
        text,
        reply_markup=markup,
    )


@router.callback_query(F.data.startswith("claim:"))
async def claim_for_campaign(callback: CallbackQuery, db):
    user_id = callback.from_user.id
    username = callback.from_user.username
    campaign_key = callback.data.split(":", 1)[1]

    recent_claim_clicks = await count_recent_abuse_events(db, user_id, "claim_click", 1)
    if recent_claim_clicks >= 3:
        await callback.answer("⏳ Слишком часто. Попробуй через минуту.", show_alert=True)
        return

    recent_claim_fails = await count_recent_abuse_events(db, user_id, "claim_fail", 10)
    if recent_claim_fails >= 10:
        await callback.answer("🚫 Слишком много неудачных попыток. Попробуй позже.", show_alert=True)
        return

    await log_abuse_event(db, user_id, "claim_click")

    ok, msg, new_balance = await claim_reward(
        db=db,
        user_id=user_id,
        username=username,
        campaign_key=campaign_key,
        first_name=callback.from_user.first_name,
        last_name=callback.from_user.last_name,
    )

    if not ok:
        await log_abuse_event(db, user_id, "claim_fail")
        await callback.answer(msg, show_alert=True)
        return

    await callback.answer(
        f"{msg}\nБаланс: {fmt_stars(new_balance)}⭐️",
        show_alert=True
    )

@router.callback_query(F.data == "withdraw")
async def withdraw_menu(callback: CallbackQuery, state: FSMContext, db):
    await callback.answer()
    await state.clear()

    user_id = callback.from_user.id
    balance = await get_balance(db, user_id)

    await callback.message.edit_text(
        "Меню заявок на вывод\n\n"
        f"Доступно: {fmt_stars(balance)}⭐",
        reply_markup=withdraw_menu_kb()
    )


@router.callback_query(F.data == "withdraw:new")
async def withdraw_new(callback: CallbackQuery, state: FSMContext, db):
    await callback.answer()
    await state.clear()

    await callback.message.edit_text(
        WITHDRAW_TEXT,
        parse_mode=ParseMode.HTML,
        reply_markup=withdraw_method_kb()
    )


def get_withdraw_fee(amount: float, is_first_withdraw: bool) -> int:
    if is_first_withdraw:
        return 0
    if amount >= 500:
        return 0
    if amount >= 200:
        return 3
    return 5


async def is_first_withdraw(db, user_id: int) -> bool:
    async with db.execute(
            "SELECT 1 FROM withdrawals WHERE user_id = ? LIMIT 1",
            (user_id,)
    ) as cur:
        row = await cur.fetchone()
    return row is None


async def validate_withdraw_rules(db, user_id: int, amount: float) -> Optional[str]:
    balance = await get_balance(db, user_id)

    if amount < MIN_WITHDRAW:
        return f"❌ Минимальная сумма вывода: {MIN_WITHDRAW:g}⭐"

    if amount > balance:
        return "❌ Недостаточно звёзд на балансе"

    user_age_hours = await user_created_hours_ago(db, user_id)
    if user_age_hours < 24:
        return "⏳ Вывод доступен только через 24 часа после регистрации."

    if await has_pending_withdrawal(db, user_id):
        return "⏳ У тебя уже есть заявка на вывод в обработке."

    recent_withdraw_count = await count_recent_abuse_events(db, user_id, "withdraw_create", 1440)
    if recent_withdraw_count >= 3:
        return "🚫 Лимит: не более 3 заявок на вывод в сутки."

    recent_withdraw_sum = await sum_recent_abuse_amount(db, user_id, "withdraw_create", 24)
    if recent_withdraw_sum + amount > 1000:
        return "🚫 Суточный лимит вывода превышен."

    earnings = await get_user_earnings_breakdown(db, user_id)
    tasks = float(earnings.get("tasks", 0) or 0)
    total = float(earnings.get("total", 0) or 0)

    if total <= 0:
        return "🚫 Вывод пока недоступен."

    tasks_pct = tasks / total
    if tasks_pct < MIN_WITHDRAW_PERCENT:
        need_more = max(0.0, total * MIN_WITHDRAW_PERCENT - tasks)
        return (
            "🚫 Вывод пока недоступен\n\n"
            f"Для вывода минимум {MIN_WITHDRAW_PERCENT * 100:.0f}% всех полученных звёзд должны быть добыты через задания.\n\n"
            f"• Всего получено: {total:.2f}⭐\n"
            f"• Через задания: {tasks:.2f}⭐ ({tasks_pct * 100:.1f}%)\n"
            f"• Нужно добрать ещё: {need_more:.2f}⭐"
        )

    return None


async def finalize_withdraw_request(
        message: Message,
        db,
        state: FSMContext,
        user_id: int,
        amount: float,
        method: str,
        wallet: Optional[str] = None,
        paid_fee: int = 0,
        fee_payment_charge_id: Optional[str] = None,
        fee_invoice_payload: Optional[str] = None,
):
    async with tx(db):
        wid = await create_withdrawal(db, user_id, amount, method=method, wallet=wallet)

        await db.execute(
            """
            UPDATE withdrawals
            SET fee_xtr = ?,
                fee_paid = ?,
                fee_refunded = 0,
                fee_telegram_charge_id = ?,
                fee_invoice_payload = ?
            WHERE id = ?
            """,
            (
                paid_fee,
                1 if paid_fee > 0 else 0,
                fee_payment_charge_id,
                fee_invoice_payload,
                wid,
            )
        )

        if paid_fee > 0:
            await xtr_ledger_add(
                db,
                user_id=user_id,
                withdrawal_id=wid,
                delta_xtr=paid_fee,
                reason="withdraw_fee_paid",
                telegram_payment_charge_id=fee_payment_charge_id,
                invoice_payload=fee_invoice_payload,
                meta=f"method={method}",
            )

        await log_abuse_event(db, user_id, "withdraw_create", amount=amount)

        ok = await apply_balance_debit_if_enough(
            db,
            user_id=user_id,
            amount=amount,
            reason="withdraw_hold",
            withdrawal_id=wid,
            meta=f"method={method};fee_xtr={paid_fee}",
        )
        if not ok:
            raise ValueError("insufficient_balance")

    username = message.from_user.username
    name = f"@{username}" if username else f"id:{user_id}"

    admin_text = (
        f"💸 Новая заявка на вывод\n\n"
        f"👤 {name}\n"
        f"⭐ {amount:g}\n"
        f"🔧 {method.upper()}\n"
    )

    if wallet:
        admin_text += f"🧾 {wallet}\n"

    if paid_fee > 0:
        admin_text += f"💳 Комиссия оплачена: {paid_fee} XTR\n"

    admin_text += f"\nID заявки: #{wid}"

    bot: Bot = message.bot
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, admin_text)
        except Exception:
            pass

        if method == "ton" and wallet:
            wallet_abuse = await wallet_used_by_another_user(db, user_id, wallet)
            if wallet_abuse:
                used_by = await wallet_users(db, wallet)
                used_by_text = "\n".join(used_by) if used_by else "нет данных"

                abuse_text = (
                    f"🚨 Подозрение на мультиаккаунт\n\n"
                    f"Новый пользователь: @{message.from_user.username or 'no_username'} (id={user_id})\n"
                    f"Кошелек:\n{wallet}\n\n"
                    f"Уже использовали:\n{used_by_text}"
                )

                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(admin_id, abuse_text)
                    except Exception:
                        pass

    await state.clear()
    new_balance = await get_balance(db, user_id)

    success_text = (
        f"✅ Заявка на вывод создана\n"
        f"ID: #{wid}\n"
        f"Сумма: {amount:g}⭐\n"
        f"Способ: {'Telegram Stars' if method == 'stars' else 'TON'}\n"
    )

    if wallet:
        success_text += f"Кошелёк: {wallet}\n"

    if paid_fee > 0:
        success_text += f"Комиссия оплачена: {paid_fee} XTR\n"

    success_text += f"\nБаланс: {fmt_stars(new_balance)}⭐"

    await message.answer(success_text)


async def start_fee_payment_or_create(
        message: Message,
        db,
        state: FSMContext,
        user_id: int,
        amount: float,
        method: str,
        wallet: Optional[str] = None,
):
    error_text = await validate_withdraw_rules(db, user_id, amount)
    if error_text:
        await message.answer(error_text)
        return

    first_withdraw = await is_first_withdraw(db, user_id)
    fee = get_withdraw_fee(amount, first_withdraw)

    if fee <= 0:
        try:
            await finalize_withdraw_request(
                message=message,
                db=db,
                state=state,
                user_id=user_id,
                amount=amount,
                method=method,
                wallet=wallet,
                paid_fee=0,
                fee_payment_charge_id=None,
                fee_invoice_payload=None,
            )
        except Exception as e:
            if isinstance(e, ValueError) and str(e) == "insufficient_balance":
                await message.answer("❌ Недостаточно звёзд на балансе")
                return
            await message.answer(f"❌ Ошибка создания заявки: {type(e).__name__}: {e}")
        return

    await state.update_data(
        amount=amount,
        method=method,
        wallet=wallet,
        withdraw_fee=fee,
    )
    await state.set_state(WithdrawCreate.fee_payment)

    await message.answer_invoice(
        title="Комиссия за вывод",
        description=f"Оплата комиссии {fee} Telegram Stars за вывод {amount:g}⭐",
        payload=f"withdraw_fee:{user_id}",
        currency="XTR",
        prices=[LabeledPrice(label="Комиссия за вывод", amount=fee)],
        provider_token="",
        start_parameter=f"withdraw-fee-{user_id}",
    )

    await message.answer(
        f"💳 Для продолжения оплати комиссию: {fee} Telegram Stars.\n"
        "После успешной оплаты заявка создастся автоматически."
    )


@router.callback_query(F.data.startswith("withdraw:method:"))
async def withdraw_choose_method(callback: CallbackQuery, state: FSMContext, db):
    await callback.answer()

    method = callback.data.split(":")[2]  # ton | stars
    await state.update_data(method=method)
    await state.set_state(WithdrawCreate.amount)

    user_id = callback.from_user.id
    balance = await get_balance(db, user_id)

    await state.clear()
    await state.update_data(method=method)

    if method == "stars":
        await callback.message.edit_text(
            "Выбери сумму вывода ⭐:\n\n"
            f"Доступно: {fmt_stars(balance)}⭐\n"
            f"Минимум: {MIN_WITHDRAW:g}⭐",
            reply_markup=withdraw_stars_amount_kb(),
        )
        return

    await state.set_state(WithdrawCreate.amount)
    await callback.message.answer(
        f"Введи сумму обмена ⭐ в TON:\n"
        f"Доступно: {fmt_stars(balance)}⭐\n"
        f"Минимум: {MIN_WITHDRAW:g}⭐"
    )


@router.callback_query(F.data.startswith("withdraw:stars_amount:"))
async def withdraw_stars_fixed_amount(callback: CallbackQuery, state: FSMContext, db):
    await callback.answer()

    user_id = callback.from_user.id
    amount = float(callback.data.split(":")[2])

    await state.update_data(method="stars", amount=amount)

    error_text = await validate_withdraw_rules(db, user_id, amount)
    if error_text:
        await callback.message.answer(error_text)
        return

    first_withdraw = await is_first_withdraw(db, user_id)
    fee = get_withdraw_fee(amount, first_withdraw)

    if fee <= 0:
        try:
            await finalize_withdraw_request(
                message=callback.message,
                db=db,
                state=state,
                user_id=user_id,
                amount=amount,
                method="stars",
                wallet=None,
                paid_fee=0,
                fee_payment_charge_id=None,
                fee_invoice_payload=None,
            )
        except Exception as e:
            if isinstance(e, ValueError) and str(e) == "insufficient_balance":
                await callback.message.answer("❌ Недостаточно звёзд на балансе")
                return
            await callback.message.answer(f"❌ Ошибка создания заявки: {type(e).__name__}: {e}")
        return

    await state.update_data(
        amount=amount,
        method="stars",
        wallet=None,
        withdraw_fee=fee,
    )
    await state.set_state(WithdrawCreate.fee_payment)

    await callback.message.answer_invoice(
        title="Комиссия за вывод",
        description=f"Оплата комиссии {fee} Telegram Stars за вывод {amount:g}⭐",
        payload=f"withdraw_fee:{user_id}",
        currency="XTR",
        prices=[LabeledPrice(label="Комиссия за вывод", amount=fee)],
        provider_token="",
        start_parameter=f"withdraw-fee-{user_id}",
    )

    await callback.message.answer(
        f"Для продолжения оплати комиссию: {fee} Telegram Stars.\n"
        "После успешной оплаты заявка создастся автоматически."
    )

@router.message(WithdrawCreate.amount)
async def withdraw_enter_amount(message: Message, state: FSMContext, db):
    data = await state.get_data()
    method = data.get("method")

    if method != "ton":
        await state.clear()
        await message.answer("❌ Для вывода в звёздах используй кнопки с фиксированной суммой.")
        return

    try:
        amount = float(message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи число > 0, например 50")
        return

    await state.update_data(amount=amount)
    await state.set_state(WithdrawCreate.wallet)
    await message.answer("Введи TON-адрес кошелька для выплаты:")

@router.message(WithdrawCreate.wallet)
async def withdraw_enter_details(message: Message, state: FSMContext, db):
    user_id = message.from_user.id
    data = await state.get_data()
    amount = float(data["amount"])
    wallet = message.text.strip()

    if len(wallet) < 10:
        await message.answer("❌ Похоже на неправильный TON-адрес. Введи ещё раз.")
        return

    await state.update_data(wallet=wallet)

    await start_fee_payment_or_create(
        message=message,
        db=db,
        state=state,
        user_id=user_id,
        amount=amount,
        method="ton",
        wallet=wallet,
    )


@router.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: PreCheckoutQuery, state: FSMContext):
    data = await state.get_data()
    fee = int(data.get("withdraw_fee") or 0)
    expected_payload = f"withdraw_fee:{pre_checkout_query.from_user.id}"

    if pre_checkout_query.invoice_payload != expected_payload:
        await pre_checkout_query.answer(
            ok=False,
            error_message="Некорректный payload оплаты."
        )
        return

    if pre_checkout_query.currency != "XTR":
        await pre_checkout_query.answer(
            ok=False,
            error_message="Некорректная валюта оплаты."
        )
        return

    if fee <= 0 or pre_checkout_query.total_amount != fee:
        await pre_checkout_query.answer(
            ok=False,
            error_message="Сумма комиссии изменилась. Открой вывод заново."
        )
        return

    await pre_checkout_query.answer(ok=True)


@router.message(F.successful_payment)
async def on_successful_payment(message: Message, state: FSMContext, db):
    payment = message.successful_payment

    print("PAYMENT:", payment)
    print("CHARGE_ID:", payment.telegram_payment_charge_id)

    if not payment:
        return

    if payment.currency != "XTR":
        return

    user_id = message.from_user.id
    data = await state.get_data()

    expected_payload = f"withdraw_fee:{user_id}"
    if payment.invoice_payload != expected_payload:
        return

    amount = float(data.get("amount") or 0)
    method = data.get("method")
    wallet = data.get("wallet")
    fee = int(data.get("withdraw_fee") or 0)

    if amount <= 0 or method not in {"stars", "ton"}:
        await message.answer(
            "⚠️ Оплата прошла, но данные заявки не найдены. Напиши администратору."
        )
        return

    if payment.total_amount != fee:
        await message.answer(
            "⚠️ Оплата прошла, но сумма комиссии не совпала. Напиши администратору."
        )
        return

    error_text = await validate_withdraw_rules(db, user_id, amount)
    if error_text:
        await message.answer(
            "⚠️ Комиссия оплачена, но заявка не может быть создана автоматически.\n\n"
            f"{error_text}\n\n"
            "Напиши администратору."
        )
        return

    try:
        await finalize_withdraw_request(
            message=message,
            db=db,
            state=state,
            user_id=user_id,
            amount=amount,
            method=method,
            wallet=wallet,
            paid_fee=fee,
            fee_payment_charge_id=payment.telegram_payment_charge_id,
            fee_invoice_payload=payment.invoice_payload,
        )
    except Exception as e:
        if isinstance(e, ValueError) and str(e) == "insufficient_balance":
            await message.answer(
                "⚠️ Комиссия оплачена, но на момент создания заявки на балансе уже не хватило звёзд.\n\n"
                "Напишите администратору."
            )
            return

        await message.answer(
            f"⚠️ Комиссия оплачена, но произошла ошибка создания заявки. Напишите администратору."
        )


@router.callback_query(F.data == "withdraw:my")
async def withdraw_my(callback: CallbackQuery, db):
    await callback.answer()
    user_id = callback.from_user.id

    rows = await user_withdrawals(db, user_id, limit=20)

    if not rows:
        await callback.message.edit_text(
            "📜 Мои заявки\n\n"
            "📭 У тебя пока нет заявок на вывод.",
            reply_markup=withdraw_back_kb()
        )
        return

    status_map = {
        "pending": "⏳ В обработке",
        "paid": "✅ Выплачено",
        "rejected": "❌ Отклонено",
    }

    lines = []
    for r in rows:
        wid, amount, method, status, created = r[0], r[1], r[2], r[3], r[4]
        lines.append(
            f"#{wid} • {float(amount):g}⭐ • {str(method).upper()} • {status_map.get(status, status)}\n"
            f"{created}"
        )

    await callback.message.edit_text(
        "📜 Мои заявки\n\n" + "\n\n".join(lines),
        reply_markup=withdraw_back_kb()
    )

@router.callback_query(F.data == "referrals")
async def show_referrals(callback: CallbackQuery, db):
    await callback.answer()

    user_id = callback.from_user.id
    invited_count = await get_referrals_count(db, user_id)

    me = await callback.bot.get_me()
    invite_link = f"https://t.me/{me.username}?start={user_id}"

    text = (
        "🫂 <b>Приглашайте друзей и получайте до 10% рефбека ⭐ с каждого их вывода!</b>\n\n"
        f"Ваша ссылка 👉🏻\n<code>{invite_link}</code>\n\n"
        f"👥 Всего приглашено: {invited_count}"
    )

    await callback.message.edit_text(
        text,
        reply_markup=referrals_kb(),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )
