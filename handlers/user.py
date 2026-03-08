from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import CommandStart, StateFilter
from aiogram.fsm.context import FSMContext

from config import CHANNEL_ID, ADMIN_IDS, MIN_WITHDRAW

from db import (
    tx, fmt_stars,
    register_user, get_balance, create_withdrawal, user_withdrawals, apply_balance_debit_if_enough,
    claim_reward, list_active_campaigns, log_abuse_event, count_recent_abuse_events, sum_recent_abuse_amount,
    has_pending_withdrawal, user_created_hours_ago, wallet_used_by_another_user, wallet_users
)

from keyboards import (
    subscribe_keyboard, main_menu, tasks_menu, bottom_menu_kb,
    withdraw_method_kb, withdraw_menu_kb, withdraw_back_kb
)

from states import WithdrawCreate

router = Router()


def menu_text(balance: float) -> str:
    return "Чтобы получить больше ⭐️, выполняйте задания\n\n" + f"Баланс: {fmt_stars(balance)}⭐️"


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

@router.message(StateFilter("*"), F.text == "🏠 Главное меню")
async def open_main_menu_from_bottom_button(message: Message, state: FSMContext, db):
    await state.clear()

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

    async with tx(db, immediate=False):
        await register_user(db, user_id, username, first_name, last_name)

    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
    except Exception:
        await message.answer("Ошибка проверки канала.")
        return

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
        await message.answer("Чтобы продолжить, подпишись на канал 👇", reply_markup=subscribe_keyboard())


@router.callback_query(F.data == "check_sub")
async def check_subscription(callback: CallbackQuery, bot: Bot, db):
    user_id = callback.from_user.id

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

    await callback.message.edit_text(
        "🚀 Задания скоро появятся\n\n"
        "Сейчас раздел находится в разработке.\n"
        "Готовим интересную механику заработка ⭐\n\n"
        "Следите за обновлениями 👀\n\n"
        f"Баланс: {fmt_stars(balance)}⭐️",
        reply_markup=tasks_menu()
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
    await callback.answer()

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
        "💸 Вывод средств\n\n"
        f"Доступно: {fmt_stars(balance)}⭐\n"
        f"Минимум: {MIN_WITHDRAW:g}⭐\n\n"
        "Выбери действие:",
        reply_markup=withdraw_menu_kb()
    )


@router.callback_query(F.data.startswith("withdraw:method:"))
async def withdraw_choose_method(callback: CallbackQuery, state: FSMContext, db):
    await callback.answer()

    method = callback.data.split(":")[2]  # ton | stars
    await state.update_data(method=method)
    await state.set_state(WithdrawCreate.amount)

    user_id = callback.from_user.id
    balance = await get_balance(db, user_id)

    await callback.message.answer(
        f"Введи сумму вывода ⭐ (число).\n"
        f"Доступно: {fmt_stars(balance)}⭐\n"
        f"Минимум: {MIN_WITHDRAW:g}⭐"
    )


@router.message(WithdrawCreate.amount)
async def withdraw_enter_amount(message: Message, state: FSMContext, db):
    user_id = message.from_user.id
    data = await state.get_data()
    method = data.get("method")

    try:
        amount = float(message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи число > 0, например 50")
        return

    balance = await get_balance(db, user_id)
    if amount < MIN_WITHDRAW:
        await message.answer(f"❌ Минимальная сумма вывода: {MIN_WITHDRAW:g}⭐")
        return
    if amount > balance:
        await message.answer("❌ Недостаточно звёзд на балансе")
        return

    user_age_hours = await user_created_hours_ago(db, user_id)
    if user_age_hours < 24:
        await message.answer("⏳ Вывод доступен только через 24 часа после регистрации.")
        return

    if await has_pending_withdrawal(db, user_id):
        await message.answer("⏳ У тебя уже есть заявка на вывод в обработке.")
        return

    recent_withdraw_count = await count_recent_abuse_events(db, user_id, "withdraw_create", 1440)
    if recent_withdraw_count >= 3:
        await message.answer("🚫 Лимит: не более 3 заявок на вывод в сутки.")
        return

    recent_withdraw_sum = await sum_recent_abuse_amount(db, user_id, "withdraw_create", 24)
    if recent_withdraw_sum + amount > 1000:
        await message.answer("🚫 Суточный лимит вывода превышен.")
        return

    await state.update_data(amount=amount)

    if method == "ton":
        await state.set_state(WithdrawCreate.details)
        await message.answer("Введи TON-адрес кошелька для выплаты:")
        return

    try:
        async with tx(db):
            wid = await create_withdrawal(db, user_id, amount, method="stars", details=None)

            await log_abuse_event(db, user_id, "withdraw_create", amount=amount)

            ok = await apply_balance_debit_if_enough(
                db,
                user_id=user_id,
                amount=amount,
                reason="withdraw_hold",
                withdrawal_id=wid,
                meta="method=stars",
            )
            if not ok:
                raise ValueError("insufficient_balance")

        username = message.from_user.username
        name = f"@{username}" if username else f"id:{user_id}"

        admin_text = (
            f"💸 Новая заявка на вывод\n\n"
            f"👤 {name}\n"
            f"⭐ {amount:g}\n"
            f"🔧 {method}\n"
            f"\nID заявки: #{wid}"
        )

        bot: Bot = message.bot
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, admin_text)
            except:
                pass

    except Exception as e:
        if isinstance(e, ValueError) and str(e) == "insufficient_balance":
            await message.answer("❌ Недостаточно звёзд на балансе")
            return
        await message.answer(f"❌ Ошибка создания заявки: {type(e).__name__}: {e}")
        return

    await state.clear()
    new_balance = await get_balance(db, user_id)
    await message.answer(
        f"✅ Заявка на вывод создана\n"
        f"ID: #{wid}\n"
        f"Сумма: {amount:g}⭐\n"
        f"Способ: Telegram Stars\n\n"
        f"Баланс: {fmt_stars(new_balance)}⭐"
    )


@router.message(WithdrawCreate.details)
async def withdraw_enter_details(message: Message, state: FSMContext, db):
    user_id = message.from_user.id
    data = await state.get_data()
    amount = float(data["amount"])
    details = message.text.strip()
    method = "ton"

    if len(details) < 10:
        await message.answer("❌ Похоже на неправильный TON-адрес. Введи ещё раз.")
        return

    try:
        async with tx(db):
            wid = await create_withdrawal(db, user_id, amount, method, details=details)

            await log_abuse_event(db, user_id, "withdraw_create", amount=amount)

            ok = await apply_balance_debit_if_enough(
                db,
                user_id=user_id,
                amount=amount,
                reason="withdraw_hold",
                withdrawal_id=wid,
                meta="method=ton",
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

        if details:
            admin_text += f"🧾 {details}\n"

        admin_text += f"\nID заявки: #{wid}"

        bot: Bot = message.bot
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, admin_text)
            except:
                pass

        wallet_abuse = await wallet_used_by_another_user(db, user_id, details)
        if wallet_abuse:
            for admin_id in ADMIN_IDS:
                used_by = await wallet_users(db, details)
                used_by_text = "\n".join(used_by) if used_by else "нет данных"

                await bot.send_message(
                    admin_id,
                    f"🚨 Подозрение на мультиаккаунт\n\n"
                    f"Новый пользователь: @{message.from_user.username or 'no_username'} (id={user_id})\n"
                    f"Кошелек:\n{details}\n\n"
                    f"Уже использовали:\n{used_by_text}"
                )

    except Exception as e:
        if isinstance(e, ValueError) and str(e) == "insufficient_balance":
            await message.answer("❌ Недостаточно звёзд на балансе")
            return
        await message.answer(f"❌ Ошибка создания заявки: {type(e).__name__}: {e}")
        return

    await state.clear()
    new_balance = await get_balance(db, user_id)
    await message.answer(
        f"✅ Заявка на вывод создана\n"
        f"ID: #{wid}\n"
        f"Сумма: {amount:g}⭐\n"
        f"Способ: TON\n"
        f"Кошелёк: {details}\n\n"
        f"Баланс: {fmt_stars(new_balance)}⭐"
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


@router.callback_query(F.data == "withdraw:new")
async def withdraw_new(callback: CallbackQuery, state: FSMContext, db):
    await callback.answer()
    await state.clear()

    user_id = callback.from_user.id
    balance = await get_balance(db, user_id)

    await callback.message.edit_text(
        "➕ Создать заявку на вывод\n\n"
        f"Доступно: {fmt_stars(balance)}⭐\n"
        f"Минимум: {MIN_WITHDRAW:g}⭐\n\n"
        "Выбери способ вывода:",
        reply_markup=withdraw_method_kb()
    )
