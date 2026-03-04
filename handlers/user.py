from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.filters import CommandStart

from config import CHANNEL_ID, ADMIN_IDS
from db import (
    register_user, get_balance,
    is_winner, attach_winner_user_id,
    has_claim, add_balance, add_claim, conn,
    list_active_campaigns, get_campaign, ledger_add
)
from keyboards import subscribe_keyboard, main_menu, tasks_menu

router = Router()

def menu_text(balance: float) -> str:
    return "Чтобы получить больше ⭐️, выполняйте задания\n\n" + f"Баланс: {balance:.2f}⭐️"


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

@router.message(CommandStart())
async def start(message: Message, bot: Bot):
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name

    register_user(user_id, username, first_name, last_name)

    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
    except Exception:
        await message.answer("Ошибка проверки канала.")
        return

    if member.status in ("member", "administrator", "creator"):
        balance = get_balance(user_id)
        await message.answer(menu_text(balance), reply_markup=main_menu(is_admin(user_id)))
    else:
        await message.answer("Чтобы продолжить, подпишись на канал 👇", reply_markup=subscribe_keyboard())

@router.callback_query(F.data == "check_sub")
async def check_subscription(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id

    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
    except Exception:
        await callback.answer("Ошибка проверки канала", show_alert=True)
        return

    if member.status in ("member", "administrator", "creator"):
        balance = get_balance(user_id)
        await callback.message.edit_text(menu_text(balance), reply_markup=main_menu(is_admin(user_id)))
    else:
        await callback.answer("❌ Ты ещё не подписан!", show_alert=True)

@router.callback_query(F.data == "tasks")
async def show_tasks(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    balance = get_balance(user_id)

    await callback.message.edit_text(
        "🚀 Задания скоро появятся\n\n"
        "Сейчас раздел находится в разработке.\n"
        "Готовим интересную механику заработка ⭐\n\n"
        "Следите за обновлениями 👀\n\n"
        f"Баланс: {balance:.2f}⭐️",
        reply_markup=tasks_menu()
    )

@router.callback_query(F.data == "back")
async def back_to_main(callback: CallbackQuery):
    user_id = callback.from_user.id
    balance = get_balance(user_id)
    await callback.answer()
    await callback.message.edit_text(menu_text(balance), reply_markup=main_menu(is_admin(user_id)))

@router.callback_query(F.data == "claim")
async def claim_menu(callback: CallbackQuery):
    campaigns = list_active_campaigns()

    if not campaigns:
        await callback.answer("❌ Сейчас нет активных конкурсов", show_alert=True)
        return

    await callback.answer()

    keyboard = []
    for key, title, amount in campaigns:
        keyboard.append([
            InlineKeyboardButton(text=f"🎁 Забрать {key}", callback_data=f"claim:{key}")
        ])

    keyboard.append([InlineKeyboardButton(text="⬅ Назад", callback_data="back")])

    await callback.message.edit_text(
        "Выбери конкурс для получения награды:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
    )

@router.callback_query(F.data.startswith("claim:"))
async def claim_for_campaign(callback: CallbackQuery):
    user_id = callback.from_user.id
    username = callback.from_user.username  # может быть None
    campaign_key = callback.data.split(":", 1)[1]

    register_user(
        user_id,
        username,
        callback.from_user.first_name,
        callback.from_user.last_name
    )

    campaign = get_campaign(campaign_key)
    if not campaign:
        await callback.answer("❌ Конкурс не найден", show_alert=True)
        return

    _key, title, reward_amount, status = campaign

    if status != "active":
        await callback.answer("❌ Этот конкурс сейчас неактивен", show_alert=True)
        return

    if username:
        attach_winner_user_id(campaign_key, username, user_id)

    if not is_winner(campaign_key, user_id, username):
        await callback.answer("❌ Ты не в списке победителей этого конкурса", show_alert=True)
        return

    if has_claim(user_id, campaign_key):
        await callback.answer("⚠️ Ты уже забрал награду в этом конкурсе", show_alert=True)
        return

    try:
        conn.execute("BEGIN")
        amount = float(reward_amount)

        add_balance(user_id, amount)
        add_claim(user_id, campaign_key, amount)

        ledger_add(
            user_id=user_id,
            delta=amount,
            reason="claim",
            campaign_key=campaign_key,
            meta=title,
        )

        conn.commit()
    except Exception:
        conn.rollback()
        await callback.answer("❌ Ошибка клейма, попробуй ещё раз", show_alert=True)
        return

    balance = get_balance(user_id)
    await callback.message.edit_text(
        f"✅ Ты получил {float(reward_amount):g}⭐️ ({title})\n\n"
        f"Баланс: {balance:.2f}⭐️",
        reply_markup=main_menu(is_admin(user_id)),
    )