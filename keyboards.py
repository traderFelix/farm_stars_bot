from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import CHANNEL_LINK


# ---------- USER KEYBOARDS ----------

def subscribe_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться", url=CHANNEL_LINK)],  # если у тебя закрытый канал — ты уже делал правильный url
            [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub")],
        ]
    )


def main_menu(is_admin: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🎁 Забрать награду", callback_data="claim")],
        [InlineKeyboardButton(text="📋 Задания", callback_data="tasks")],
    ]
    if is_admin:
        rows.append([InlineKeyboardButton(text="🛠 Админка", callback_data="adm:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def tasks_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Назад", callback_data="back")],
        ]
    )


# ---------- ADMIN KEYBOARDS ----------

def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Все конкурсы", callback_data="adm:list")],
            [InlineKeyboardButton(text="➕ Создать конкурс", callback_data="adm:new")],
            [InlineKeyboardButton(text="📊 Статистика", callback_data="adm:stats_menu")],
            [InlineKeyboardButton(text="🏆 Топ по балансу", callback_data="adm:top")],
            [InlineKeyboardButton(text="⛔ Закрыть", callback_data="adm:close")],
        ]
    )


def admin_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Назад", callback_data="adm:back")]
        ]
    )


def _status_icon(status: str) -> str:
    if status == "active":
        return "🟢"
    if status == "ended":
        return "🔴"
    if status == "draft":
        return "🟡"
    return "⚪"


def campaigns_list_kb(rows) -> InlineKeyboardMarkup:
    keyboard = []
    for key, amount, status, created_at in rows[:50]:
        icon = _status_icon(str(status))
        keyboard.append([
            InlineKeyboardButton(
                text=f"{icon} {key} — {float(amount):g}⭐",
                callback_data=f"adm:open:{key}"
            )
        ])

    keyboard.append([InlineKeyboardButton(text="⬅ Назад", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def stats_list_kb(rows) -> InlineKeyboardMarkup:
    keyboard = []
    for key, amount, status, created_at in rows:
        icon = _status_icon(str(status))
        keyboard.append([
            InlineKeyboardButton(
                text=f"{icon} {key}",
                callback_data=f"adm:stats:{key}"
            )
        ])

    keyboard.append([InlineKeyboardButton(text="⬅ Назад", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def campaign_manage_kb(key: str, status: str) -> InlineKeyboardMarkup:
    keyboard = []

    if status == "active":
        keyboard.append([InlineKeyboardButton(text="🔴 Выключить", callback_data=f"adm:off:{key}")])
    else:
        keyboard.append([InlineKeyboardButton(text="🟢 Включить", callback_data=f"adm:on:{key}")])

    keyboard.append([
        InlineKeyboardButton(text="➕ Добавить победителей", callback_data=f"adm:add_winners:{key}"),
        InlineKeyboardButton(text="👥 Победители", callback_data=f"adm:show_winners:{key}"),
    ])

    keyboard.append([
        InlineKeyboardButton(text="➖ Удалить победителя", callback_data=f"adm:winner_del:{key}"),
        InlineKeyboardButton(text="🗑 Удалить конкурс", callback_data=f"adm:del:{key}"),
    ])

    keyboard.append([
        InlineKeyboardButton(text="⬅ Назад", callback_data="adm:list"),
    ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def campaign_created_kb(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📂 Открыть конкурс", callback_data=f"adm:open:{key}")],
            [InlineKeyboardButton(text="📋 Все конкурсы", callback_data="adm:list")],
        ]
    )