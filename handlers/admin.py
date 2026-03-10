import io
from datetime import date, timedelta

import matplotlib
matplotlib.use("Agg")  # важно для серверов без GUI

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, TelegramObject, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.filters import Filter
from aiogram.methods import RefundStarPayment
from aiogram.exceptions import TelegramBadRequest

from config import ADMIN_IDS

from handlers.user import menu_text, is_admin

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
    ledger_add, ledger_user_history, apply_balance_delta, get_balance, balances_audit,

    # withdraw
    list_withdrawals, get_withdrawal, set_withdrawal_status,
)

from keyboards import (
    main_menu, admin_menu_kb, admin_back_kb, campaigns_list_kb, campaign_manage_kb, stats_list_kb,
    campaign_created_kb, admin_user_kb, admin_withdraw_list_kb, admin_withdraw_actions_kb, campaign_delete_confirm_kb,
)

from states import CampaignCreate, AddWinners, DeleteWinner, UserLookup, AdminAdjust

router = Router()


class AdminOnly(Filter):
    async def __call__(self, event: TelegramObject) -> bool:
        user = getattr(event, "from_user", None)
        return bool(user and user.id in ADMIN_IDS)


router.message.filter(AdminOnly())
router.callback_query.filter(AdminOnly())


async def _render_campaign_card(callback: CallbackQuery, key: str, db):
    row = await get_campaign(db, key)
    if not row:
        await callback.message.edit_text("❌ Конкурс не найден.", reply_markup=admin_back_kb())
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

    await callback.message.edit_text(
        f"🏷 {key}\n"
        f"📝 {title}\n"
        f"🎁 Награда: {amount}⭐\n"
        f"📌 Статус: {status_text}",
        reply_markup=campaign_manage_kb(key, status),
    )


@router.callback_query(F.data == "adm:back")
async def adm_back(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("🛠 Админ-панель", reply_markup=admin_menu_kb())


@router.callback_query(F.data == "adm:close")
async def adm_close(callback: CallbackQuery, db):
    await callback.answer()

    user_id = callback.from_user.id
    balance = await get_balance(db, user_id)

    await callback.message.edit_text(
        menu_text(balance),
        reply_markup=main_menu(is_admin(user_id))
    )


@router.callback_query(F.data == "adm:list")
async def adm_list(callback: CallbackQuery, db):
    await callback.answer()

    rows = await list_campaigns(db)
    if not rows:
        await callback.message.edit_text("Пока нет конкурсов.", reply_markup=admin_back_kb())
        return

    await callback.message.edit_text("📋 Список всех конкурсов:", reply_markup=campaigns_list_kb(rows))


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
        await callback.message.edit_text(
            "❌ Конкурс не найден.",
            reply_markup=admin_back_kb()
        )
        return

    _k, title, amount, status = row[0], row[1], row[2], row[3]

    await callback.message.edit_text(
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
    await callback.message.edit_text(
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
        await callback.message.edit_text("Нет конкурсов", reply_markup=admin_back_kb())
        return

    total_assigned_sum = await total_assigned_amount(db)
    claims_count_all, total_claimed_all = await global_claims_stats(db)
    active_cnt, ended_cnt, draft_cnt = await campaigns_status_counts(db)
    unclaimed_sum = await unclaimed_total_amount(db)

    await callback.message.edit_text(
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

    await callback.message.edit_text(
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

    await callback.message.edit_text(
        f"🏆 Победители конкурса {key}:\n\n{text}",
        reply_markup=back_kb
    )


@router.callback_query(F.data == "adm:home")
async def adm_home(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("🛠 Админ-панель", reply_markup=admin_menu_kb())


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

    await callback.message.edit_text(text, reply_markup=admin_back_kb())


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

    await callback.message.answer_photo(photo=photo, caption=caption)

    await callback.message.edit_text(
        "📈 График и цифры отправил сообщением выше.",
        reply_markup=admin_back_kb()
    )


@router.callback_query(F.data == "adm:ledger_last")
async def adm_ledger_last(callback: CallbackQuery, db):
    await callback.answer()

    async with db.execute(
            """
        SELECT l.created_at, u.username, l.delta, l.reason, l.campaign_key
        FROM ledger l
        LEFT JOIN users u ON u.user_id = l.user_id
        ORDER BY datetime(l.created_at) DESC
        LIMIT 30
        """
    ) as cur:
        rows = await cur.fetchall()

    if not rows:
        await callback.message.edit_text("📜 Леджер пуст.", reply_markup=admin_back_kb())
        return

    lines = []
    for r in rows:
        created_at, username, delta, reason, campaign_key = r[0], r[1], r[2], r[3], r[4]
        name = f"@{username}" if username else "(no-username)"
        ck = f" [{campaign_key}]" if campaign_key else ""
        lines.append(f"{created_at} — {name}: {float(delta):g}⭐ — {reason}{ck}")

    await callback.message.edit_text(
        "📜 Последние 30 операций:\n\n" + "\n".join(lines),
        reply_markup=admin_back_kb()
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
        await callback.message.edit_text(
            "✅ Нет заявок на вывод (pending).",
            reply_markup=admin_back_kb()
        )
        return

    await callback.message.edit_text(
        "💸 Заявки на вывод (pending):",
        reply_markup=admin_withdraw_list_kb(rows)
    )


async def _render_withdraw_card(callback: CallbackQuery, wid: int, db):
    row = await get_withdrawal(db, wid)
    if not row:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    _id, user_id, username, amount, method, details, status, created_at = (
        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]
    )

    name = f"@{username}" if username else f"id:{user_id}"
    det = details or "—"

    await callback.message.edit_text(
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

    _id, user_id, username, amount, method, details, status, created_at = (
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

            await ledger_add(
                db,
                user_id=user_id,
                delta=0.0,
                reason="withdraw_paid",
                withdrawal_id=wid,
                meta=f"method={method}",
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
    row = await db.fetchone(
        """
        SELECT user_id, fee_xtr, fee_paid, fee_refunded, fee_telegram_charge_id
        FROM withdrawals
        WHERE id = ?
        """,
        (withdrawal_id,)
    )
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

    await db.execute(
        """
        UPDATE withdrawals
        SET fee_refunded = 1
        WHERE id = ?
        """,
        (withdrawal_id,)
    )
    await db.commit()

    return True, "refunded"


@router.callback_query(F.data.startswith("adm:wd:reject:"))
async def adm_withdraw_reject(callback: CallbackQuery, db):
    wid = int(callback.data.split(":")[3])
    admin_id = callback.from_user.id

    row = await get_withdrawal(db, wid)
    if not row:
        await callback.answer("❌ Заявка не найдена", show_alert=True)
        return

    _id, user_id, username, amount, method, details, status, created_at = (
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

        try:
            await callback.bot.send_message(
                int(user_id),
                f"❌ Твоя заявка на вывод #{wid} отклонена.\n"
                f"Сумма: {float(amount):g}⭐ возвращена на баланс."
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
    claimed_from_ledger = await ledger_sum_by_reason(db, "claim")

    lines = [
        "🧮 Сверка балансов\n",
        f"Баланс пользователей: {fmt_stars(total_balances_sum)}⭐\n",
        f"Получено в конкурсах (база): {fmt_stars(total_claimed_all)}⭐",
        f"Получено в конкурсах (леджер): {fmt_stars(claimed_from_ledger)}⭐",
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
            user_id, balance, ledger_sum = row[0], row[1], row[2]
            lines.append(
                f"user_id={user_id}: balance={fmt_stars(balance)}⭐ / ledger={fmt_stars(ledger_sum)}⭐"
            )

    await callback.message.edit_text(
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
        await callback.message.edit_text(
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

    await callback.message.edit_text(
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

    await callback.message.edit_text(
        text,
        reply_markup=admin_user_kb(user_id),
    )
    await callback.answer("Подозрение снято")

@router.callback_query(F.data.startswith("adm:user:ledger:"))
async def adm_user_ledger(callback: CallbackQuery, db):
    try:
        user_id = int(callback.data.split(":")[-1])
    except (ValueError, IndexError):
        await callback.answer("Некорректный user_id", show_alert=True)
        return

    history = await ledger_user_history(db, user_id)

    lines = []
    for r in history:
        created_at, delta, reason, campaign_key = r[0], r[1], r[2], r[3]
        ck = f" ({campaign_key})" if campaign_key else ""
        lines.append(f"{created_at}: {float(delta):g}⭐ {reason}{ck}")

    if not lines:
        lines = ["нет операций"]

    text = (
            f"📜 Последние операции\n\n"
            + "\n".join(lines)
    )

    await callback.message.edit_text(
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
        await callback.message.edit_text(
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
