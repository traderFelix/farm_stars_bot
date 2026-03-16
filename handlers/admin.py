import io, logging
from datetime import date, timedelta

import matplotlib
matplotlib.use("Agg")  # важно для серверов без GUI

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from aiogram.enums import ParseMode
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, TelegramObject, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.filters import Filter
from aiogram.methods import RefundStarPayment
from aiogram.exceptions import TelegramBadRequest

from config import ADMIN_IDS, LEDGER_PAGE_SIZE

from handlers.user import menu_text, is_admin, safe_edit_text

from db import (
    tx, fmt_stars,

    # campaigns
    upsert_campaign, set_campaign_status, delete_campaign, list_campaigns, list_campaigns_latest, get_campaign,
    add_winners, delete_winner_if_not_claimed,

    # stats
    campaign_stats, list_winners, claimed_usernames, global_claims_stats, campaigns_status_counts, total_balances,
    unclaimed_total_amount, total_assigned_amount, admin_balance_changes, total_withdrawn_amount, pending_withdrawn_amount,
    ledger_sum_by_reason, build_user_stats_text,

    # users/growth
    top_users_by_balance, users_total_count, users_new_since_hours, users_new_since_days, users_active_since_days,
    users_growth_by_day, build_user_details_text, mark_user_suspicious, clear_user_suspicious,

    # ledger
    ledger_add, apply_balance_delta, get_balance, balances_audit, xtr_ledger_add,

    # withdraw
    list_withdrawals, get_withdrawal, set_withdrawal_status, mark_withdraw_fee_refunded, list_recent_fee_payments,
    find_withdraw_by_fee_charge_id, add_referral_bonus_for_paid_withdrawal,

    # channels
    list_task_channels, get_task_channel, create_task_channel, set_task_channel_active, task_channel_stats, update_task_channel_params,
    get_task_channel_allocated_views, list_task_posts_by_channel
)

from keyboards import (
    main_menu, admin_menu_kb, admin_back_kb, campaigns_list_kb, campaign_manage_kb, stats_list_kb, admin_fee_refund_kb,
    campaign_created_kb, admin_user_kb, admin_withdraw_list_kb, admin_withdraw_actions_kb, campaign_delete_confirm_kb,
    admin_task_channels_kb, admin_task_channel_card_kb, admin_growth_photo_kb,
)

from states import (
    CampaignCreate, AddWinners, DeleteWinner, UserLookup, AdminAdjust, AdminRefundFee, TaskChannelCreate, TaskChannelEdit,
)

router = Router()

logger = logging.getLogger(__name__)

class AdminOnly(Filter):
    async def __call__(self, event: TelegramObject) -> bool:
        user = getattr(event, "from_user", None)
        return bool(user and user.id in ADMIN_IDS)


router.message.filter(AdminOnly())
router.callback_query.filter(AdminOnly())


def _admin_ledger_nav_kb(page: int, has_next: bool) -> InlineKeyboardMarkup:
    row = []
    if page > 0:
        row.append(
            InlineKeyboardButton(
                text="⬅ Пред",
                callback_data=f"adm:ledger_last:{page - 1}",
            )
        )
    if has_next:
        row.append(
            InlineKeyboardButton(
                text="След ➡",
                callback_data=f"adm:ledger_last:{page + 1}",
            )
        )

    keyboard = []
    if row:
        keyboard.append(row)

    keyboard.append([
        InlineKeyboardButton(text="⬅ Назад", callback_data="adm:back")
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def _user_ledger_nav_kb(user_id: int, page: int, has_next: bool) -> InlineKeyboardMarkup:
    row = []
    if page > 0:
        row.append(
            InlineKeyboardButton(
                text="⬅ Пред",
                callback_data=f"adm:user:ledger:{user_id}:{page - 1}",
            )
        )
    if has_next:
        row.append(
            InlineKeyboardButton(
                text="След ➡",
                callback_data=f"adm:user:ledger:{user_id}:{page + 1}",
            )
        )

    keyboard = []
    if row:
        keyboard.append(row)

    keyboard.append([
        InlineKeyboardButton(
            text="⬅ Назад",
            callback_data=f"adm:user:details:{user_id}",
        )
    ])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


async def _render_campaign_card(callback: CallbackQuery, key: str, db):
    row = await get_campaign(db, key)
    if not row:
        await safe_edit_text(callback.message, "❌ Конкурс не найден.", reply_markup=admin_back_kb())
        return

    _k, title, amount, status = row[0], row[1], row[2], row[3]

    if status == "active":
        status_text = "🟢 Активен"
    elif status == "draft":
        status_text = "🟡 Черновик"
    elif status == "ended":
        status_text = "🔴 Завершён"
    else:
        status_text = f"⚪ {status}"

    text = f"🏷 {key}\n" \
            f"📝 {title}\n" \
            f"🎁 Награда: {amount}⭐\n" \
            f"📌 Статус: {status_text}"
    await safe_edit_text(callback.message, text, reply_markup=campaign_manage_kb(key, status))


@router.callback_query(F.data == "adm:back")
async def adm_back(callback: CallbackQuery):
    await callback.answer()
    await safe_edit_text(callback.message, "🛠 Админ-панель", reply_markup=admin_menu_kb())


@router.callback_query(F.data == "adm:close")
async def adm_close(callback: CallbackQuery, db):
    await callback.answer()

    user_id = callback.from_user.id
    balance = await get_balance(db, user_id)

    await safe_edit_text(
        callback.message,
        menu_text(balance),
        reply_markup=main_menu(is_admin(user_id))
    )


@router.callback_query(F.data == "adm:list")
async def adm_list(callback: CallbackQuery, db):
    await callback.answer()

    rows = await list_campaigns(db)
    if not rows:
        await safe_edit_text(callback.message, "Пока нет конкурсов.", reply_markup=admin_back_kb())
        return

    await safe_edit_text(callback.message, "📋 Список всех конкурсов:", reply_markup=campaigns_list_kb(rows))


@router.callback_query(F.data.startswith("adm:open:"))
async def adm_open(callback: CallbackQuery, db):
    await callback.answer()
    key = callback.data.split(":", 2)[2]
    await _render_campaign_card(callback, key, db)


@router.callback_query(F.data.startswith("adm:on:"))
async def adm_on(callback: CallbackQuery, db):
    await callback.answer()
    key = callback.data.split(":", 2)[2]
    async with tx(db, immediate=False):
        await set_campaign_status(db, key, "active")
    await _render_campaign_card(callback, key, db)


@router.callback_query(F.data.startswith("adm:off:"))
async def adm_off(callback: CallbackQuery, db):
    await callback.answer()
    key = callback.data.split(":", 2)[2]
    async with tx(db, immediate=False):
        await set_campaign_status(db, key, "ended")
    await _render_campaign_card(callback, key, db)


@router.callback_query(F.data.startswith("adm:del:ask:"))
async def adm_delete_ask(callback: CallbackQuery, db):
    await callback.answer()
    key = callback.data.split(":", 3)[3]

    row = await get_campaign(db, key)
    if not row:
        await safe_edit_text(
            callback.message,
            "❌ Конкурс не найден.",
            reply_markup=admin_back_kb()
        )
        return

    _k, title, amount, status = row[0], row[1], row[2], row[3]

    await safe_edit_text(
        callback.message,
        f"⚠️ Ты точно хочешь удалить конкурс?\n\n"
        f"KEY: {key}\n"
        f"Название: {title}\n"
        f"Награда: {amount}⭐\n"
        f"Статус: {status}",
        reply_markup=campaign_delete_confirm_kb(key),
    )


@router.callback_query(F.data.startswith("adm:del:do:"))
async def adm_delete_do(callback: CallbackQuery, db):
    await callback.answer()
    key = callback.data.split(":", 3)[3]

    async with tx(db):
        await delete_campaign(db, key)

    await adm_list(callback, db)


@router.callback_query(F.data.startswith("adm:add_winners:"))
async def add_winners_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    key = callback.data.split(":")[2]
    await state.set_state(AddWinners.usernames)
    await state.update_data(campaign_key=key)

    await callback.message.answer(
        f"Введи username победителей для конкурса: {key}\n\n"
        "@username1\n"
        "@username2"
    )


@router.message(AddWinners.usernames)
async def save_winners_msg(message: Message, state: FSMContext, db):

    data = await state.get_data()
    key = data.get("campaign_key")
    usernames = [
        line.strip().lstrip("@")
        for line in (message.text or "").splitlines()
        if line.strip()
    ]

    async with tx(db):
        count = await add_winners(db, key, usernames)

    await state.clear()
    await message.answer(f"✅ Добавлено {count} победителей к конкурсу {key}")


@router.callback_query(F.data == "adm:new")
async def adm_new(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(CampaignCreate.key)
    await safe_edit_text(
        callback.message,
        "➕ Создание конкурса\n\nВведи KEY (например: march2026):",
        reply_markup=admin_back_kb(),
    )


@router.message(CampaignCreate.key)
async def adm_new_key(message: Message, state: FSMContext):
    key = (message.text or "").strip()
    if " " in key or len(key) < 3:
        await message.answer("❌ KEY без пробелов, минимум 3 символа. Введи снова:")
        return
    await state.update_data(key=key)
    await state.set_state(CampaignCreate.amount)
    await message.answer("Теперь введи награду (число), например: 10")


@router.message(CampaignCreate.amount)
async def adm_new_amount(message: Message, state: FSMContext):
    try:
        amount = float((message.text or "").strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Нужна награда числом > 0. Пример: 10")
        return
    await state.update_data(amount=amount)
    await state.set_state(CampaignCreate.title)
    await message.answer("И последнее — введи название конкурса (title):")


@router.message(CampaignCreate.title)
async def adm_new_title(message: Message, state: FSMContext, db):
    title = (message.text or "").strip()
    data = await state.get_data()
    key = data["key"]
    amount = data["amount"]

    async with tx(db):
        await upsert_campaign(db, key, title, amount, "draft")

    await state.clear()

    await message.answer(
        f"✅ Конкурс создан:\n"
        f"🏷 {key}\n"
        f"🎁 {amount}⭐\n"
        f"📝 {title}\n"
        f"Статус: 🟡 Черновик",
        reply_markup=campaign_created_kb(key)
    )


@router.callback_query(F.data == "adm:stats_menu")
async def adm_stats_menu(callback: CallbackQuery, db):
    await callback.answer()

    rows = await list_campaigns_latest(db, limit=5)
    if not rows:
        await safe_edit_text(callback.message, "Нет конкурсов", reply_markup=admin_back_kb())
        return

    total_assigned_sum = await total_assigned_amount(db)
    claims_count_all, total_claimed_all = await global_claims_stats(db)
    active_cnt, ended_cnt, draft_cnt = await campaigns_status_counts(db)
    unclaimed_sum = await unclaimed_total_amount(db)

    await safe_edit_text(
        callback.message,
        "📊 Полная статистика:\n\n"
        f"🎁 Начислено в конкурсах: {total_assigned_sum:.2f}⭐\n"
        f"📦 Невостребовано: {unclaimed_sum:.2f}⭐\n"
        f"💰 Всего заклеймили: {total_claimed_all:.2f}⭐\n ({claims_count_all} клеймов)\n"
        f"🟡 Черновиков: {draft_cnt}\n"
        f"🟢 Активных конкурсов: {active_cnt}\n"
        f"🔴 Завершённых: {ended_cnt}\n\n"
        "Последние 5 конкурсов:",
        reply_markup=stats_list_kb(rows)
    )


@router.callback_query(F.data.startswith("adm:stats:"))
async def adm_stats(callback: CallbackQuery, db):
    await callback.answer()

    key = callback.data.split(":")[2]

    claims_count, winners_cnt, total_paid = await campaign_stats(db, key)
    claimed = await claimed_usernames(db, key)

    if claimed:
        claimed_text = "\n".join([f"@{u}" for u in claimed[:50]])
        if len(claimed) > 50:
            claimed_text += f"\n… и ещё {len(claimed) - 50}"
    else:
        claimed_text = "—"

    await safe_edit_text(
        callback.message,
        f"📊 Статистика конкурса {key}\n\n"
        f"👥 Клеймов: {claims_count}/{winners_cnt}\n"
        f"⭐ Выплачено всего: {total_paid}\n\n"
        f"✅ Заклеймили:\n{claimed_text}",
        reply_markup=admin_back_kb()
    )


@router.callback_query(F.data.startswith("adm:show_winners:"))
async def adm_show_winners(callback: CallbackQuery, db):
    await callback.answer()

    key = callback.data.split(":")[2]

    winners = await list_winners(db, key)
    claimed = set(await claimed_usernames(db, key))

    back_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅ Назад", callback_data=f"adm:open:{key}")]
        ]
    )

    if not winners:
        text = "Победителей нет"
    else:
        lines = []
        for i, u in enumerate(winners[:50], start=1):
            mark = " ✅" if u in claimed else ""
            lines.append(f"{i}. @{u}{mark}")
        text = "\n".join(lines)

    await safe_edit_text(
        callback.message,
        f"🏆 Победители конкурса {key}:\n\n{text}",
        reply_markup=back_kb
    )


@router.callback_query(F.data == "adm:home")
async def adm_home(callback: CallbackQuery):
    await callback.answer()
    await safe_edit_text(callback.message, "🛠 Админ-панель", reply_markup=admin_menu_kb())


@router.callback_query(F.data.startswith("adm:winner_del:"))
async def winner_del_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    key = callback.data.split(":")[2]

    await state.set_state(DeleteWinner.username)
    await state.update_data(campaign_key=key)

    await callback.message.answer(
        f"➖ Удаление победителя из конкурса {key}\n\n"
        "Введи username:"
    )


@router.message(DeleteWinner.username)
async def winner_del_finish(message: Message, state: FSMContext, db):
    data = await state.get_data()
    key = data["campaign_key"]
    username = (message.text or "").strip()

    async with tx(db):
        ok, msg = await delete_winner_if_not_claimed(db, key, username)

    await state.clear()

    if ok:
        await message.answer(f"✅ Удалил {username} из победителей конкурса {key}")
    else:
        await message.answer(f"⚠️ {msg}")


@router.callback_query(F.data == "adm:top")
async def adm_top_balances(callback: CallbackQuery, db):
    await callback.answer()

    rows = await top_users_by_balance(db, 10)

    if not rows:
        text = "🏆 Топ-10 по балансу:\n\nПока нет пользователей с балансом ⭐️"
    else:
        lines = []
        for i, r in enumerate(rows, start=1):
            username, balance = r[0], r[1]
            name = f"@{username}" if username else "(без username)"
            lines.append(f"{i}. {name} — {float(balance):.2f}⭐️")
        text = "🏆 Топ-10 по балансу:\n\n" + "\n".join(lines)

    await safe_edit_text(callback.message, text, reply_markup=admin_back_kb())


@router.callback_query(F.data == "adm:growth_png")
async def adm_growth_png(callback: CallbackQuery, db):
    await callback.answer()

    days = 30
    total = await users_total_count(db)
    new_1d = await users_new_since_hours(db, 24)
    new_7d = await users_new_since_days(db, 7)
    new_30d = await users_new_since_days(db, 30)

    active_1d = await users_active_since_days(db, 1)
    active_7d = await users_active_since_days(db, 7)
    active_30d = await users_active_since_days(db, 30)

    points = await users_growth_by_day(db, days)

    fig = plt.figure()
    ax = fig.add_subplot(111)

    if points:
        data = {d: int(cnt) for d, cnt in points}

        xs = [(date.today() - timedelta(days=days - 1 - i)).isoformat() for i in range(days)]
        ys = [data.get(d, 0) for d in xs]

        ax.bar(xs, ys)
        ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
        ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%d"))

        ax.set_ylim(bottom=0)
        ax.set_xticks(xs[::max(1, len(xs) // 15)])
        ax.set_xlabel("Date")
        ax.set_ylabel("New users")
        ax.set_title(f"User growth (last {days} days)")

        fig.autofmt_xdate(rotation=45)
    else:
        ax.text(0.5, 0.5, "No data yet", ha="center", va="center")
        ax.set_axis_off()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)

    photo = BufferedInputFile(buf.read(), filename="growth.png")

    caption = (
        f"📈Рост пользователей\n\n"
        f"👥Всего: {total}\n\n"
        f"🆕Новые:\n"
        f"1д - {new_1d}\n"
        f"7д - {new_7d}\n"
        f"30д - {new_30d}\n\n"
        f"🔥 Активные:\n"
        f"1д - {active_1d}\n"
        f"7д - {active_7d}\n"
        f"30д - {active_30d}"
    )

    origin_message_id = callback.message.message_id

    await callback.message.answer_photo(
        photo=photo,
        caption=caption,
        reply_markup=admin_growth_photo_kb(origin_message_id),
    )


@router.callback_query(F.data.startswith("adm:ledger_last"))
async def adm_ledger_last(callback: CallbackQuery, db):
    await callback.answer()

    parts = (callback.data or "").split(":")
    page = 0
    if len(parts) >= 3:
        try:
            page = max(int(parts[2]), 0)
        except ValueError:
            page = 0

    offset = page * LEDGER_PAGE_SIZE

    async with db.execute(
            """
        SELECT l.created_at, u.username, l.delta, l.reason, l.campaign_key
        FROM ledger l
        LEFT JOIN users u ON u.user_id = l.user_id
        ORDER BY l.created_at DESC, l.id DESC
        LIMIT ? OFFSET ?
        """,
            (LEDGER_PAGE_SIZE + 1, offset),
    ) as cur:
        rows = await cur.fetchall()

    if not rows and page == 0:
        await safe_edit_text(
            callback.message,
            "📜 Леджер пуст.",
            reply_markup=admin_back_kb()
        )
        return

    if not rows and page > 0:
        return

    has_next = len(rows) > LEDGER_PAGE_SIZE
    rows = rows[:LEDGER_PAGE_SIZE]

    lines = []
    start_n = offset + 1

    for i, r in enumerate(rows, start=start_n):
        created_at, username, delta, reason, campaign_key = r
        name = f"@{username}" if username else "(no-username)"
        ck = f" [{campaign_key}]" if campaign_key else ""
        lines.append(
            f"{i}. {created_at} — {name}: {float(delta):g}⭐ — {reason}{ck}"
        )

    await safe_edit_text(
        callback.message,
        f"📜 Леджер, страница {page + 1}:\n\n" + "\n".join(lines),
        reply_markup=_admin_ledger_nav_kb(page, has_next),
        )


@router.callback_query(F.data == "adm:user_balance")
async def adm_user_balance(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(UserLookup.user)
    await callback.message.answer("Введи username или user_id пользователя:")


@router.message(UserLookup.user)
async def adm_user_balance_show(message: Message, state: FSMContext, db):
    value = (message.text or "").strip()

    if value.isdigit():
        user_id = int(value)
    else:
        username = value.lstrip("@")
        async with db.execute(
                "SELECT user_id FROM users WHERE username = ? LIMIT 1",
                (username,),
        ) as cur:
            row = await cur.fetchone()

        if not row:
            await message.answer("❌ Пользователь не найден")
            return

        user_id = int(row[0])

    text = await build_user_details_text(db, user_id)

    await message.answer(
        text,
        reply_markup=admin_user_kb(user_id),
    )
    await state.clear()


@router.callback_query(F.data.startswith("adm:ub:add:"))
async def adm_user_add_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user_id = int(callback.data.split(":")[3])

    await state.update_data(adj_user_id=user_id, adj_mode="add")
    await state.set_state(AdminAdjust.amount)

    await callback.message.answer("Введите сумму ⭐ для начисления:")


@router.callback_query(F.data.startswith("adm:ub:sub:"))
async def adm_user_sub_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user_id = int(callback.data.split(":")[3])

    await state.update_data(adj_user_id=user_id, adj_mode="sub")
    await state.set_state(AdminAdjust.amount)

    await callback.message.answer("Введите сумму ⭐ для списания:")


@router.message(AdminAdjust.amount)
async def adm_user_adjust_finish(message: Message, state: FSMContext, db):
    data = await state.get_data()
    user_id = int(data["adj_user_id"])
    mode = data["adj_mode"]

    try:
        amount = float((message.text or "").strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи число > 0, например 10")
        return

    delta = amount if mode == "add" else -amount

    try:
        async with tx(db):
            await apply_balance_delta(
                db,
                user_id=user_id,
                delta=delta,
                reason="admin_adjust",
                meta=f"mode={mode}",
            )
    except Exception:
        await message.answer("❌ Ошибка операции, попробуй ещё раз")
        logger.exception(Exception)
        return

    balance = await get_balance(db, user_id)
    await state.clear()

    await message.answer(
        f"✅ Готово\n"
        f"Изменение: {delta:+.2f}⭐\n"
        f"Новый баланс: {fmt_stars(balance)}⭐",
        reply_markup=admin_user_kb(user_id)
    )


@router.callback_query(F.data == "adm:wd:list")
async def adm_withdraw_list(callback: CallbackQuery, db):
    await callback.answer()

    rows = await list_withdrawals(db, status="pending", limit=20)

    if not rows:
        await safe_edit_text(
            callback.message,
            "✅ Нет заявок на вывод (pending).",
            reply_markup=admin_back_kb()
        )
        return

    await safe_edit_text(
        callback.message,
        "💸 Заявки на вывод (pending):",
        reply_markup=admin_withdraw_list_kb(rows)
    )


async def _render_withdraw_card(callback: CallbackQuery, wid: int, db):
    row = await get_withdrawal(db, wid)
    if not row:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    _id, user_id, username, amount, method, wallet, status, created_at = (
        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]
    )

    name = f"@{username}" if username else f"id:{user_id}"
    det = wallet or "—"

    await safe_edit_text(
        callback.message,
        f"💸 Заявка #{_id}\n\n"
        f"👤 {name}\n"
        f"⭐ Сумма: {float(amount):g}\n"
        f"🔧 Метод: {method}\n"
        f"🧾 Детали: {det}\n"
        f"📌 Статус: {status}\n"
        f"🕒 Создано: {created_at}",
        reply_markup=admin_withdraw_actions_kb(_id)
    )


@router.callback_query(F.data.startswith("adm:wd:open:"))
async def adm_withdraw_open(callback: CallbackQuery, db):
    await callback.answer()
    wid = int(callback.data.split(":")[3])
    await _render_withdraw_card(callback, wid, db)


@router.callback_query(F.data.startswith("adm:wd:paid:"))
async def adm_withdraw_paid(callback: CallbackQuery, db):
    await callback.answer()

    wid = int(callback.data.split(":")[3])
    admin_id = callback.from_user.id

    row = await get_withdrawal(db, wid)
    if not row:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    _id, user_id, username, amount, method, wallet, status, created_at = (
        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]
    )

    if status != "pending":
        await callback.answer("⚠️ Уже обработана", show_alert=True)
        return

    try:
        async with tx(db):
            row2 = await get_withdrawal(db, wid)
            if not row2:
                await callback.answer("❌ Заявка не найдена", show_alert=True)
                return

            status2 = row2[6]
            if status2 != "pending":
                await callback.answer("⚠️ Уже обработана", show_alert=True)
                return

            await set_withdrawal_status(db, wid, "paid", admin_id)

            logger.info(
                "WITHDRAW PAID | wid=%s user_id=%s amount=%s",
                wid, user_id, amount
            )

            await ledger_add(
                db,
                user_id=user_id,
                delta=0.0,
                reason="withdraw_paid",
                withdrawal_id=wid,
                meta=f"method={method}",
            )

            try:
                bonus_added, referrer_id, bonus_amount = await add_referral_bonus_for_paid_withdrawal(
                    db,
                    referred_user_id=int(user_id),
                    withdrawal_id=int(wid),
                    withdraw_amount=float(amount),
                )
                logger.info(
                    "REF BONUS CHECK | wid=%s referred_user_id=%s bonus_added=%s referrer_id=%s bonus_amount=%s",
                    wid, user_id, bonus_added, referrer_id, bonus_amount
                )
            except Exception:
                logger.exception(
                    "Failed to add referral bonus: wid=%s referred_user_id=%s amount=%s",
                    wid, user_id, amount
                )
                raise

            if bonus_added and referrer_id and bonus_amount > 0:
                try:
                    await callback.bot.send_message(
                        referrer_id,
                        f"🎉 Ваш друг вывел {float(amount):g}⭐.\n"
                        f"Вы получили рефбек: {bonus_amount:g}⭐"
                    )
                except Exception:
                    logger.exception("Failed to notify referrer %s for withdrawal %s", referrer_id, wid)

            logger.info(
                "REF BONUS SENT | wid=%s referrer_id=%s referred_user_id=%s bonus_amount=%s",
                wid, referrer_id, user_id, bonus_amount
            )

        try:
            await callback.bot.send_message(
                user_id,
                f"✅ Твоя заявка на вывод #{wid} выплачена.\n"
                f"Сумма: {float(amount):g}⭐\n"
                f"Метод: {str(method).upper()}"
            )
        except Exception:
            pass  # юзер мог заблокировать бота / закрыть ЛС

    except Exception as e:
        await callback.answer(f"❌ Ошибка: {type(e).__name__}: {e}", show_alert=True)
        return

    await callback.answer("✅ Отмечено как выплачено", show_alert=True)
    await _render_withdraw_card(callback, wid, db)


async def refund_withdraw_fee_if_needed(bot: Bot, db, withdrawal_id: int) -> tuple[bool, str]:
    async with db.execute(
        """
        SELECT user_id, fee_xtr, fee_paid, fee_refunded, fee_telegram_charge_id
        FROM withdrawals
        WHERE id = ?
        """,
        (withdrawal_id,)
    ) as cur:
        row = await cur.fetchone()

    if not row:
        return False, "withdrawal_not_found"

    user_id = int(row["user_id"])
    fee_xtr = int(row["fee_xtr"] or 0)
    fee_paid = int(row["fee_paid"] or 0)
    fee_refunded = int(row["fee_refunded"] or 0)
    charge_id = row["fee_telegram_charge_id"]

    if fee_xtr <= 0 or not fee_paid:
        return True, "no_fee_paid"

    if fee_refunded:
        return True, "already_refunded"

    if not charge_id:
        return False, "missing_charge_id"

    ok = await bot(
        RefundStarPayment(
            user_id=user_id,
            telegram_payment_charge_id=charge_id,
        )
    )

    if not ok:
        return False, "refund_failed"

    await mark_withdraw_fee_refunded(db, withdrawal_id)

    await xtr_ledger_add(
        db,
        user_id=int(user_id),
        withdrawal_id=withdrawal_id,
        delta_xtr=-int(fee_xtr),
        reason="withdraw_fee_refunded",
        telegram_payment_charge_id=charge_id,
        meta="status=rejected",
    )

    return True, "refunded"


@router.callback_query(F.data.startswith("adm:wd:reject:"))
async def adm_withdraw_reject(callback: CallbackQuery, db):
    wid = int(callback.data.split(":")[3])
    admin_id = callback.from_user.id
    bot = callback.bot

    row = await get_withdrawal(db, wid)
    if not row:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    _id, user_id, username, amount, method, wallet, status, created_at = (
        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]
    )

    if status != "pending":
        await callback.answer("⚠️ Уже обработана", show_alert=True)
        return

    try:
        async with tx(db):
            row2 = await get_withdrawal(db, wid)
            if not row2:
                await callback.answer("❌ Заявка не найдена", show_alert=True)
                return

            status2 = row2[6]
            if status2 != "pending":
                await callback.answer("⚠️ Уже обработана", show_alert=True)
                return

            await set_withdrawal_status(db, wid, "rejected", admin_id)
            await apply_balance_delta(
                db,
                user_id=int(user_id),
                delta=float(amount),
                reason="withdraw_release",
                withdrawal_id=int(wid),
                meta="rejected",
            )

        fee_refund_text = ""

        refunded, refund_status = await refund_withdraw_fee_if_needed(bot, db, wid)

        if refund_status == "refunded":
            fee_row = await get_withdrawal(db, wid)
            fee_xtr = int(fee_row["fee_xtr"] or 0)
            fee_refund_text = f"\nКомиссия {fee_xtr}⭐ возвращена."

        elif refund_status == "refund_failed":
            fee_refund_text = "\n⚠️ Комиссию вернуть не удалось."

        elif refund_status == "missing_charge_id":
            fee_refund_text = "\n⚠️ У комиссии нет charge_id, вернуть автоматически не удалось."

        elif refund_status == "withdrawal_not_found":
            fee_refund_text = "\n⚠️ Заявка на вывод не найдена."

        try:
            await callback.bot.send_message(
                int(user_id),
                f"❌ Твоя заявка на вывод #{wid} отклонена.\n"
                f"Сумма: {float(amount):g}⭐ возвращена на баланс."
                f"{fee_refund_text}"
            )
        except Exception:
            pass

    except Exception as e:
        await callback.answer(f"❌ Ошибка: {type(e).__name__}: {e}", show_alert=True)
        return

    await _render_withdraw_card(callback, wid, db)
    await callback.answer("✅ Отклонено и возвращено на баланс", show_alert=True)


@router.callback_query(F.data == "adm:audit")
async def adm_audit_balances(callback: CallbackQuery, db):
    await callback.answer()

    mismatches = await balances_audit(db)
    total_balances_sum = await total_balances(db)
    claims_count_all, total_claimed_all = await global_claims_stats(db)
    admin_added, admin_removed = await admin_balance_changes(db)
    total_withdrawn_sum = await total_withdrawn_amount(db)
    pending_withdrawn_sum = await pending_withdrawn_amount(db)
    claimed_from_ledger = await ledger_sum_by_reason(db, "contest_bonus")
    referral_bonus = await ledger_sum_by_reason(db, "referral_bonus")

    lines = [
        "🧮 Сверка балансов\n",
        f"Баланс пользователей: {fmt_stars(total_balances_sum)}⭐\n",
        f"Получено в конкурсах (база): {fmt_stars(total_claimed_all)}⭐",
        f"Получено в конкурсах (леджер): {fmt_stars(claimed_from_ledger)}⭐",
        f"Получено за рефералов: {fmt_stars(referral_bonus)}⭐\n"
        f"Получено от админа: {fmt_stars(admin_added - admin_removed)}⭐",
        f"Выведено: {fmt_stars(total_withdrawn_sum)}⭐",
        f"В обработке: {fmt_stars(pending_withdrawn_sum)}⭐\n",
    ]

    if not mismatches:
        lines.append("✅ Расхождений не найдено")
    else:
        lines.append(f"⚠️ Найдено расхождений: {len(mismatches)}")
        lines.append("")
        lines.append("Первые 10:")
        for row in mismatches[:10]:
            user_id = row["user_id"]
            username = row["username"]
            balance = row["users_balance"]
            ledger_sum = row["ledger_sum"]
            diff = row["diff"]

            uname = f"@{username}" if username else "без username"

            lines.append(
                f"user_id={user_id} ({uname}): "
                f"balance={fmt_stars(balance)}⭐ / "
                f"ledger={fmt_stars(ledger_sum)}⭐ / "
                f"diff={fmt_stars(diff)}⭐"
            )

    await safe_edit_text(
        callback.message,
        "\n".join(lines),
        reply_markup=admin_back_kb(),
    )

@router.callback_query(F.data.startswith("adm:user:details:"))
async def adm_user_details(callback: CallbackQuery, db):
    try:
        user_id = int(callback.data.split(":")[-1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный user_id", show_alert=True)
        return

    text = await build_user_details_text(db, user_id)

    try:
        await safe_edit_text(
            callback.message,
            text,
            reply_markup=admin_user_kb(user_id),
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise

    await callback.answer()

@router.callback_query(F.data.startswith("adm:user:mark_susp:"))
async def adm_user_mark_susp(callback: CallbackQuery, db):
    try:
        user_id = int(callback.data.split(":")[-1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный user_id", show_alert=True)
        return

    await mark_user_suspicious(db, user_id, "Помечен администратором")
    text = await build_user_details_text(db, user_id)

    await safe_edit_text(
        callback.message,
        text,
        reply_markup=admin_user_kb(user_id),
    )
    await callback.answer("Пользователь помечен")


@router.callback_query(F.data.startswith("adm:user:clear_susp:"))
async def adm_user_clear_susp(callback: CallbackQuery, db):
    try:
        user_id = int(callback.data.split(":")[-1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный user_id", show_alert=True)
        return

    await clear_user_suspicious(db, user_id)
    text = await build_user_details_text(db, user_id)

    await safe_edit_text(
        callback.message,
        text,
        reply_markup=admin_user_kb(user_id),
    )
    await callback.answer("Подозрение снято")

@router.callback_query(F.data.startswith("adm:user:ledger:"))
async def adm_user_ledger(callback: CallbackQuery, db):
    parts = (callback.data or "").split(":")

    try:
        user_id = int(parts[3])
    except (ValueError, IndexError):
        await callback.answer("Некорректный user_id", show_alert=True)
        return

    page = 0
    if len(parts) >= 5:
        try:
            page = max(int(parts[4]), 0)
        except ValueError:
            page = 0

    offset = page * LEDGER_PAGE_SIZE

    async with db.execute(
            """
        SELECT created_at, delta, reason, campaign_key
        FROM ledger
        WHERE user_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT ? OFFSET ?
        """,
            (user_id, LEDGER_PAGE_SIZE + 1, offset),
    ) as cur:
        history = await cur.fetchall()

    if not history and page > 0:
        await callback.answer("Дальше записей нет")
        return

    has_next = len(history) > LEDGER_PAGE_SIZE
    history = history[:LEDGER_PAGE_SIZE]

    lines = []
    start_n = offset + 1

    for i, r in enumerate(history, start=start_n):
        created_at, delta, reason, campaign_key = r
        ck = f" ({campaign_key})" if campaign_key else ""
        lines.append(f"{i}. {created_at}: {float(delta):g}⭐ {reason}{ck}")

    if not lines:
        lines = ["нет операций"]

    text = (
            f"📜 Операции пользователя, страница {page + 1}\n\n"
            + "\n".join(lines)
    )

    await safe_edit_text(
        callback.message,
        text,
        reply_markup=_user_ledger_nav_kb(user_id, page, has_next),
    )
    await callback.answer()

@router.callback_query(F.data.startswith("adm:user:stats:"))
async def adm_user_stats(callback: CallbackQuery, db):
    try:
        user_id = int(callback.data.split(":")[-1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный user_id", show_alert=True)
        return

    text = await build_user_stats_text(db, user_id)

    try:
        await safe_edit_text(
            callback.message,
            text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="⬅ Назад",
                            callback_data=f"adm:user:details:{user_id}",
                        )
                    ]
                ]
            ),
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise

    await callback.answer()

@router.callback_query(F.data == "adm:fee_refund_menu")
async def adm_fee_refund_menu(callback: CallbackQuery, db):
    await callback.answer()

    rows = await list_recent_fee_payments(db, limit=10)

    if not rows:
        await safe_edit_text(
            callback.message,
            "↩️ Возврат комиссии\n\n"
            "Пока нет последних оплат комиссии.",
            reply_markup=admin_fee_refund_kb(),
        )
        return

    lines = ["↩️ Возврат комиссии\n", "Последние 10 оплат:\n"]

    for i, row in enumerate(reversed(rows), start=1):
        withdrawal_id = row["withdrawal_id"]
        user_id = row["user_id"]
        username = row["username"]
        fee_xtr = row["fee_xtr"]
        fee_paid = int(row["fee_paid"] or 0)
        fee_refunded = int(row["fee_refunded"] or 0)
        charge_id = row["fee_telegram_charge_id"] or "-"
        created_at = row["created_at"]

        status = "возвращено" if fee_refunded else ("оплачено" if fee_paid else "не оплачено")
        uname_line = f"@{username}" if username else "без username"

        lines.append(
            f"wid={withdrawal_id} {uname_line}\n"
            f"fee={fee_xtr}⭐\n"
            f"status={status}\n"
            f"created_at={created_at}\n"
            f"<code>{user_id} {charge_id}</code>\n"
        )

    await callback.message.edit_text(
        "\n".join(lines),
        parse_mode=ParseMode.HTML,
        reply_markup=admin_fee_refund_kb(),
    )

@router.callback_query(F.data == "adm:fee_refund_manual")
async def adm_fee_refund_manual(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(AdminRefundFee.waiting_manual_data)

    await callback.message.answer(
        "Введи параметры для возврата в таком формате:\n\n"
        "user_id charge_id\n"
        )

@router.message(AdminRefundFee.waiting_manual_data)
async def adm_fee_refund_manual_finish(message: Message, state: FSMContext, db):
    text = (message.text or "").strip()
    parts = text.split(maxsplit=1)

    if len(parts) != 2:
        await message.answer(
            "❌ Неверный формат.\n\n"
            "Нужно так:\n"
            "user_id charge_id"
        )
        return

    user_id_raw, charge_id = parts

    try:
        user_id = int(user_id_raw)
    except ValueError:
        await message.answer("❌ user_id должен быть числом.")
        return

    try:
        ok = await message.bot(
            RefundStarPayment(
                user_id=user_id,
                telegram_payment_charge_id=charge_id,
            )
        )
    except TelegramBadRequest as e:
        await message.answer(f"❌ TelegramBadRequest: {e}")
        return
    except Exception as e:
        await message.answer(f"❌ Ошибка возврата: {type(e).__name__}: {e}")
        return

    if not ok:
        await message.answer("❌ Telegram вернул неуспешный результат.")
        return

    row = await find_withdraw_by_fee_charge_id(db, charge_id)
    if row and not int(row["fee_refunded"] or 0):
        await mark_withdraw_fee_refunded(db, row["withdrawal_id"])

        await xtr_ledger_add(
            db,
            user_id=int(row["user_id"]),
            withdrawal_id=int(row["withdrawal_id"]),
            delta_xtr=-int(row["fee_xtr"] or 0),
            reason="withdraw_fee_refunded",
            telegram_payment_charge_id=charge_id,
            meta="status=manual_refund",
        )
    elif not row:
        await message.answer(
            "⚠️ Refund в Telegram выполнен, но заявка по charge_id не найдена. "
            "В xtr_ledger запись не добавлена."
        )

    await state.clear()
    await message.answer(
        "✅ Комиссия успешно возвращена.\n"
        f"user_id={user_id}\n"
        f"charge_id={charge_id}"
    )

async def _render_task_channel_card(callback: CallbackQuery, channel_id: int, db):
    row = await get_task_channel(db, channel_id)
    if not row:
        await safe_edit_text(callback.message, "❌ Канал не найден.", reply_markup=admin_back_kb())
        return

    stats = await task_channel_stats(db, channel_id)

    title = row["title"] or "Без названия"
    chat_id = row["chat_id"]
    is_active = int(row["is_active"] or 0) == 1
    total_bought = int(row["total_bought_views"] or 0)
    views_per_post = int(row["views_per_post"] or 0)
    allocated = int(row["allocated_views"] or 0)
    remaining = int(row["remaining_views"] or 0)
    view_seconds = int(row["view_seconds"] or 0)
    total_posts = int(stats["total_posts"] or 0)
    total_required = int(stats["total_required"] or 0)
    total_current = int(stats["total_current"] or 0)
    active_posts = int(stats["active_posts"] or 0)

    status_text = "🟢 Включен" if is_active else "🔴 Отключен"

    await safe_edit_text(
        callback.message,
        "📺 Канал просмотров\n\n"
        f"Название: {title}\n"
        f"ID канала: {chat_id}\n"
        f"Статус: {status_text}\n\n"
        f"Куплено просмотров: {total_bought}\n"
        f"На один пост: {views_per_post}\n"
        f"Секунд просмотра: {view_seconds}\n"
        f"Уже распределено: {allocated}\n"
        f"Осталось распределить: {remaining}\n\n"
        f"Постов в системе: {total_posts}\n"
        f"Активных постов: {active_posts}\n"
        f"Всего нужно просмотров по постам: {total_required}\n"
        f"Фактически набрано: {total_current}",
        reply_markup=admin_task_channel_card_kb(channel_id, is_active),
    )

@router.callback_query(F.data == "adm:tch:list")
async def adm_task_channels_list(callback: CallbackQuery, db):
    await callback.answer()
    rows = await list_task_channels(db)

    if not rows:
        await safe_edit_text(
            callback.message,
            "📺 Каналы просмотров\n\n"
            "Пока нет подключенных каналов.",
            reply_markup=admin_task_channels_kb([]),
        )
        return

    await safe_edit_text(
        callback.message,
        "📺 Каналы просмотров\n\n"
        "Выбери канал:",
        reply_markup=admin_task_channels_kb(rows),
    )


@router.callback_query(F.data == "adm:tch:new")
async def adm_task_channel_new_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(TaskChannelCreate.chat_id)
    await safe_edit_text(
        callback.message,
        "➕ Подключение канала\n\n"
        "Пришли chat_id канала.\n"
        "Пример: -1001234567890",
        reply_markup=admin_back_kb(),
    )


@router.message(TaskChannelCreate.chat_id)
async def adm_task_channel_new_chat_id(message: Message, state: FSMContext):
    chat_id = (message.text or "").strip()

    if not chat_id.startswith("-100"):
        await message.answer("❌ Нужен channel id в формате -100...")
        return

    await state.update_data(chat_id=chat_id)
    await state.set_state(TaskChannelCreate.total_bought_views)
    await message.answer("Теперь введи, сколько просмотров куплено всего для этого канала:")


@router.message(TaskChannelCreate.total_bought_views)
async def adm_task_channel_new_total_views(message: Message, state: FSMContext):
    try:
        total_bought_views = int((message.text or "").strip())
        if total_bought_views <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число больше 0.")
        return

    await state.update_data(total_bought_views=total_bought_views)
    await state.set_state(TaskChannelCreate.views_per_post)
    await message.answer("Теперь введи, сколько просмотров выделять на 1 пост:")


@router.message(TaskChannelCreate.views_per_post)
async def adm_task_channel_new_views_per_post(message: Message, state: FSMContext):
    try:
        views_per_post = int((message.text or "").strip())
        if views_per_post <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число больше 0.")
        return

    data = await state.get_data()
    total_bought_views = int(data["total_bought_views"])

    if views_per_post > total_bought_views:
        await message.answer("❌ Просмотров на 1 пост не может быть больше, чем куплено всего.")
        return

    await state.update_data(views_per_post=views_per_post)
    await state.set_state(TaskChannelCreate.view_seconds)
    await message.answer("Теперь введи, сколько секунд держать пост перед засчитыванием просмотра:")


@router.callback_query(F.data.startswith("adm:tch:open:"))
async def adm_task_channel_open(callback: CallbackQuery, db):
    await callback.answer()
    channel_id = int(callback.data.split(":")[3])
    await _render_task_channel_card(callback, channel_id, db)


@router.callback_query(F.data.startswith("adm:tch:toggle:"))
async def adm_task_channel_toggle(callback: CallbackQuery, db):
    await callback.answer()
    channel_id = int(callback.data.split(":")[3])

    row = await get_task_channel(db, channel_id)
    if not row:
        await safe_edit_text(callback.message, "❌ Канал не найден.", reply_markup=admin_back_kb())
        return

    new_active = 0 if int(row["is_active"] or 0) == 1 else 1

    async with tx(db):
        await set_task_channel_active(db, channel_id, new_active)

    await _render_task_channel_card(callback, channel_id, db)

@router.callback_query(F.data.startswith("adm:tch:edit:"))
async def adm_task_channel_edit_start(callback: CallbackQuery, state: FSMContext, db):
    await callback.answer()
    channel_id = int(callback.data.split(":")[3])

    row = await get_task_channel(db, channel_id)
    if not row:
        await safe_edit_text(callback.message, "❌ Канал не найден.", reply_markup=admin_back_kb())
        return

    await state.set_state(TaskChannelEdit.total_bought_views)
    await state.update_data(channel_id=channel_id)

    await safe_edit_text(
        callback.message,
        "⚙️ Редактирование параметров канала\n\n"
        f"Текущий chat_id: {row['chat_id']}\n"
        f"Сейчас куплено просмотров: {int(row['total_bought_views'] or 0)}\n"
        f"Сейчас просмотров на 1 пост: {int(row['views_per_post'] or 0)}\n"
        f"Сейчас секунд просмотра: {int(row['view_seconds'] or 0)}\n"
        f"Уже распределено по постам: {int(row['allocated_views'] or 0)}\n\n"
        "Введи новое общее количество купленных просмотров:",
        reply_markup=admin_back_kb(),
    )

@router.message(TaskChannelEdit.total_bought_views)
async def adm_task_channel_edit_total_views(message: Message, state: FSMContext, db):
    try:
        total_bought_views = int((message.text or "").strip())
        if total_bought_views <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число больше 0.")
        return

    data = await state.get_data()
    channel_id = int(data["channel_id"])

    allocated_views = await get_task_channel_allocated_views(db, channel_id)
    if total_bought_views < allocated_views:
        await message.answer(
            "❌ Нельзя поставить меньше, чем уже распределено по постам.\n\n"
            f"Уже распределено: {allocated_views}"
        )
        return

    await state.update_data(total_bought_views=total_bought_views)
    await state.set_state(TaskChannelEdit.views_per_post)
    await message.answer("Теперь введи новое количество просмотров на 1 пост:")

@router.message(TaskChannelEdit.views_per_post)
async def adm_task_channel_edit_views_per_post(message: Message, state: FSMContext, db):
    try:
        views_per_post = int((message.text or "").strip())
        if views_per_post <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число больше 0.")
        return

    data = await state.get_data()
    channel_id = int(data["channel_id"])
    total_bought_views = int(data["total_bought_views"])

    if views_per_post > total_bought_views:
        await message.answer("❌ Просмотров на 1 пост не может быть больше, чем куплено всего.")
        return

    await state.update_data(views_per_post=views_per_post)
    await state.set_state(TaskChannelEdit.view_seconds)
    await message.answer("Теперь введи новое количество секунд просмотра:")


@router.callback_query(F.data.startswith("adm:tch:posts:"))
async def adm_task_channel_posts(callback: CallbackQuery, db):
    await callback.answer()
    channel_id = int(callback.data.split(":")[3])

    channel = await get_task_channel(db, channel_id)
    if not channel:
        await safe_edit_text(callback.message, "❌ Канал не найден.", reply_markup=admin_back_kb())
        return

    rows = await list_task_posts_by_channel(db, channel_id, limit=20)

    title = channel["title"] or channel["chat_id"]

    if not rows:
        await safe_edit_text(
            callback.message,
            "📊 Статус по постам\n\n"
            f"Канал: {title}\n\n"
            "Пока нет добавленных постов.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅ Назад к каналу", callback_data=f"adm:tch:open:{channel_id}")],
                    [InlineKeyboardButton(text="📺 Все каналы", callback_data="adm:tch:list")],
                ]
            )
        )
        return

    lines = []
    for row in rows:
        post_id = int(row["channel_post_id"])
        current_views = int(row["current_views"] or 0)
        required_views = int(row["required_views"] or 0)

        done = current_views >= required_views and required_views > 0
        status = "✅" if done else "🔄"

        created_at = row["created_at"] or "-"
        lines.append(
            f"📝 Пост #{post_id} ({created_at}) — {current_views}/{required_views} {status}\n"
        )

    text = (
            "📊 Статус по постам\n\n"
            f"Канал: {title}\n\n"
            + "\n".join(lines)
    )

    await safe_edit_text(
        callback.message,
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅ Назад к каналу", callback_data=f"adm:tch:open:{channel_id}")],
                [InlineKeyboardButton(text="📺 Все каналы", callback_data="adm:tch:list")],
            ]
        )
    )

@router.message(TaskChannelEdit.view_seconds)
async def adm_task_channel_edit_view_seconds(message: Message, state: FSMContext, db):
    try:
        view_seconds = int((message.text or "").strip())
        if view_seconds <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число больше 0.")
        return

    data = await state.get_data()
    channel_id = int(data["channel_id"])
    total_bought_views = int(data["total_bought_views"])
    views_per_post = int(data["views_per_post"])

    async with tx(db):
        await update_task_channel_params(
            db=db,
            channel_id=channel_id,
            total_bought_views=total_bought_views,
            views_per_post=views_per_post,
            view_seconds=view_seconds,
        )

    await state.clear()

    row = await get_task_channel(db, channel_id)
    if not row:
        await message.answer("✅ Параметры обновлены.")
        return

    stats = await task_channel_stats(db, channel_id)

    title = row["title"] or "Без названия"
    chat_id = row["chat_id"]
    is_active = int(row["is_active"] or 0) == 1
    allocated = int(row["allocated_views"] or 0)
    remaining = int(row["remaining_views"] or 0)

    total_posts = int(stats["total_posts"] or 0)
    active_posts = int(stats["active_posts"] or 0)
    total_required = int(stats["total_required"] or 0)
    total_current = int(stats["total_current"] or 0)

    status_text = "🟢 Включен" if is_active else "🔴 Отключен"

    await message.answer(
        "✅ Параметры канала обновлены\n\n"
        f"Название: {title}\n"
        f"chat_id: {chat_id}\n"
        f"Статус: {status_text}\n\n"
        f"Куплено просмотров: {int(row['total_bought_views'] or 0)}\n"
        f"На 1 пост: {int(row['views_per_post'] or 0)}\n"
        f"Секунд просмотра: {int(row['view_seconds'] or 0)}\n"
        f"Уже распределено: {allocated}\n"
        f"Осталось распределить: {remaining}\n\n"
        f"Постов в системе: {total_posts}\n"
        f"Активных постов: {active_posts}\n"
        f"Всего нужно просмотров по постам: {total_required}\n"
        f"Фактически набрано: {total_current}",
        reply_markup=admin_task_channel_card_kb(channel_id, is_active),
    )

@router.message(TaskChannelCreate.view_seconds)
async def adm_task_channel_new_view_seconds(message: Message, state: FSMContext, db):
    try:
        view_seconds = int((message.text or "").strip())
        if view_seconds <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число больше 0.")
        return

    data = await state.get_data()
    chat_id = data["chat_id"]
    total_bought_views = int(data["total_bought_views"])
    views_per_post = int(data["views_per_post"])

    async with tx(db):
        new_id = await create_task_channel(
            db=db,
            chat_id=chat_id,
            title=None,
            total_bought_views=total_bought_views,
            views_per_post=views_per_post,
            view_seconds=view_seconds,
        )

    await state.clear()

    await message.answer(
        "✅ Канал подключен\n\n"
        f"chat_id: {chat_id}\n"
        f"Куплено просмотров: {total_bought_views}\n"
        f"На 1 пост: {views_per_post}\n"
        f"Секунд просмотра: {view_seconds}"
    )

    await message.answer(
        "Открой канал:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📺 Открыть канал", callback_data=f"adm:tch:open:{new_id}")],
                [InlineKeyboardButton(text="📺 Все каналы", callback_data="adm:tch:list")],
            ]
        )
    )

@router.callback_query(F.data.startswith("adm:growth_back:"))
async def adm_growth_back(callback: CallbackQuery):
    await callback.answer()

    origin_message_id = int(callback.data.rsplit(":", 1)[1])

    try:
        await callback.message.delete()
    except Exception:
        pass

    try:
        await callback.bot.edit_message_text(
            chat_id=callback.from_user.id,
            message_id=origin_message_id,
            text="🔐 Админ-панель",
            reply_markup=admin_menu_kb(),
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
