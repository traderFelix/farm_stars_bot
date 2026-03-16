from aiogram.types import InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from config import CHANNEL_LINK, ROLE_CLIENT, ROLE_PARTNER, ROLE_ADMIN

def bottom_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Главное меню")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        selective=False,
        input_field_placeholder=""
    )

# ---------- USER KEYBOARDS ----------

def subscribe_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub")],
        ]
    )

def main_menu(role_level: int = 0) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🎁 Забрать награду", callback_data="claim")],
        [InlineKeyboardButton(text="📋 Задания", callback_data="tasks")],
        [InlineKeyboardButton(text="👛 Вывод", callback_data="withdraw")],
        [InlineKeyboardButton(text="🫂 Пригласить друга", callback_data="referrals")],
    ]
    if role_level >= ROLE_CLIENT:
        rows.append([InlineKeyboardButton(text="🤝 Кабинет клиента", callback_data="client:home")])
    if role_level >= ROLE_PARTNER:
        rows.append([InlineKeyboardButton(text="💼 Кабинет парнера", callback_data="partner:home")])
    if role_level >= ROLE_ADMIN:
        rows.append([InlineKeyboardButton(text="🔐 Админка", callback_data="adm:home")])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def referrals_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Назад", callback_data="back")]
        ]
    )

def withdraw_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать заявку", callback_data="withdraw:new")],
        [InlineKeyboardButton(text="📜 Мои заявки", callback_data="withdraw:my")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="back")],
    ])

def withdraw_back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад", callback_data="withdraw")],
    ])

def withdraw_method_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ Вывести звезды", callback_data="withdraw:method:stars")],
        [InlineKeyboardButton(text="🔄 Обменять звезды на TON", callback_data="withdraw:method:ton")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="withdraw")],
    ])

def tasks_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👁 Смотреть пост", callback_data="task:view_post")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="back")],
        ]
    )

def task_after_view_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👁 Смотреть следующий пост", callback_data="task:view_post")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="back")],
        ]
    )

# ---------- ADMIN KEYBOARDS ----------

def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Все конкурсы", callback_data="adm:list")],
            [InlineKeyboardButton(text="➕ Создать конкурс", callback_data="adm:new")],
            [InlineKeyboardButton(text="📊 Статистика конкурсов", callback_data="adm:stats_menu")],
            [InlineKeyboardButton(text="📺 Каналы просмотров", callback_data="adm:tch:list")],
            [InlineKeyboardButton(text="📈 Рост пользователей", callback_data="adm:growth_png")],
            [InlineKeyboardButton(text="📜 Леджер (последние)", callback_data="adm:ledger_last")],
            [InlineKeyboardButton(text="🔎 Детали пользователя", callback_data="adm:user_balance")],
            [InlineKeyboardButton(text="🏆 Топ по балансу", callback_data="adm:top")],
            [InlineKeyboardButton(text="💸 Заявки на вывод", callback_data="adm:wd:list")],
            [InlineKeyboardButton(text="↩️ Возврат комсы", callback_data="adm:fee_refund_menu")],
            [InlineKeyboardButton(text="🧮 Сверка балансов", callback_data="adm:audit")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="back")],
        ]
    )

def admin_withdraw_list_kb(rows):
    kb = []
    for wid, user_id, username, amount, method, wallet, status, created_at in rows:
        name = f"@{username}" if username else f"id:{user_id}"
        kb.append([InlineKeyboardButton(
            text=f"#{wid} {name} — {float(amount):g}⭐ ({method})",
            callback_data=f"adm:wd:open:{wid}"
        )])
    kb.append([InlineKeyboardButton(text="⬅ Назад", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def admin_withdraw_actions_kb(withdrawal_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Выплатил", callback_data=f"adm:wd:paid:{withdrawal_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"adm:wd:reject:{withdrawal_id}")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="adm:wd:list")],
    ])

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

def admin_user_kb(user_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статистика ⭐", callback_data=f"adm:user:stats:{user_id}",)],
            [InlineKeyboardButton(text="📜 Последние операции", callback_data=f"adm:user:ledger:{user_id}",)],
            [InlineKeyboardButton(text="➕ Начислить ⭐", callback_data=f"adm:ub:add:{user_id}",)],
            [InlineKeyboardButton(text="➖ Списать ⭐", callback_data=f"adm:ub:sub:{user_id}",)],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="adm:back",)],
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
        InlineKeyboardButton(text="🗑 Удалить конкурс", callback_data=f"adm:del:ask:{key}"),
    ])

    keyboard.append([
        InlineKeyboardButton(text="⬅ Назад", callback_data="adm:list"),
    ])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def campaign_delete_confirm_kb(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"adm:del:do:{key}")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data=f"adm:open:{key}")],
        ]
    )

def campaign_created_kb(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📂 Открыть конкурс", callback_data=f"adm:open:{key}")],
            [InlineKeyboardButton(text="📋 Все конкурсы", callback_data="adm:list")],
        ]
    )

def admin_user_details_kb(user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👤 Детали пользователя", callback_data=f"adm:user:details:{user_id}")
    builder.button(text="⬅️ Назад", callback_data="adm:users")
    builder.adjust(1)
    return builder.as_markup()


def admin_fee_refund_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Вернуть вручную", callback_data="adm:fee_refund_manual")],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="adm:back")],
        ]
    )

def withdraw_stars_amount_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="100⭐", callback_data="withdraw:stars_amount:100"),
            InlineKeyboardButton(text="200⭐", callback_data="withdraw:stars_amount:200"),
            InlineKeyboardButton(text="500⭐", callback_data="withdraw:stars_amount:500"),
            InlineKeyboardButton(text="1000⭐", callback_data="withdraw:stars_amount:1000"),
        ],
        [
            InlineKeyboardButton(text="⬅ Назад", callback_data="withdraw:new"),
        ],
    ])

def admin_task_channels_kb(rows) -> InlineKeyboardMarkup:
    kb = []

    for row in rows:
        channel_id = int(row["id"])
        title = row["title"] or row["chat_id"]
        is_active = int(row["is_active"] or 0)
        remaining = int(row["remaining_views"] or 0)
        status = "🟢" if is_active else "🔴"
        kb.append([
            InlineKeyboardButton(
                text=f"{status} {title} • остаток {remaining}",
                callback_data=f"adm:tch:open:{channel_id}",
            )
        ])

    kb.append([InlineKeyboardButton(text="➕ Подключить канал", callback_data="adm:tch:new")])
    kb.append([InlineKeyboardButton(text="⬅ Назад", callback_data="adm:back")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def admin_task_channel_card_kb(channel_id: int, is_active: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Статус по постам", callback_data=f"adm:tch:posts:{channel_id}")],
            [InlineKeyboardButton(text="⚙️ Редактировать параметры", callback_data=f"adm:tch:edit:{channel_id}")],
            [InlineKeyboardButton(
                text="🔴 Отключить канал" if is_active else "🟢 Включить канал",
                callback_data=f"adm:tch:toggle:{channel_id}",
            )],
            [InlineKeyboardButton(text="⬅ Назад", callback_data="adm:tch:list")],
        ]
    )

def admin_growth_photo_kb(origin_message_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="⬅ Назад",
                callback_data=f"adm:growth_back:{origin_message_id}"
            )]
        ]
    )

