import io
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

from datetime import date, timedelta

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, TelegramObject, BufferedInputFile
from aiogram.fsm.context import FSMContext
from aiogram.filters import Filter

from config import ADMIN_IDS

from handlers.user import menu_text, is_admin

from db import campaign_stats, list_winners, claimed_usernames, list_campaigns_latest
from db import (
    users_growth_by_day, users_total_count, upsert_campaign, set_campaign_status, delete_campaign, list_campaigns, get_campaign, add_winners, get_balance, total_balances,
    global_claims_stats, campaigns_status_counts, unclaimed_total_amount, total_assigned_amount, delete_winner_if_not_claimed, top_users_by_balance, users_new_since_hours,
    users_new_since_days, users_active_since_days
)
from keyboards import main_menu, admin_menu_kb, admin_back_kb, campaigns_list_kb, campaign_manage_kb, stats_list_kb, campaign_created_kb

from states import CampaignCreate, AddWinners, DeleteWinner

router = Router()

class AdminOnly(Filter):
    async def __call__(self, event: TelegramObject) -> bool:
        user = getattr(event, "from_user", None)
        return bool(user and user.id in ADMIN_IDS)

router.message.filter(AdminOnly())
router.callback_query.filter(AdminOnly())


async def _render_campaign_card(callback: CallbackQuery, key: str):
    row = get_campaign(key)
    if not row:
        await callback.message.edit_text("❌ Конкурс не найден.", reply_markup=admin_back_kb())
        return
    
    _k, title, amount, status = row

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
async def adm_close(callback: CallbackQuery):
    await callback.answer()

    user_id = callback.from_user.id
    balance = get_balance(user_id)

    await callback.message.edit_text(
        menu_text(balance),
        reply_markup=main_menu(is_admin(user_id))
    )

@router.callback_query(F.data == "adm:list")
async def adm_list(callback: CallbackQuery):
    await callback.answer()
    rows = list_campaigns()
    if not rows:
        await callback.message.edit_text("Пока нет конкурсов.", reply_markup=admin_back_kb())
        return
    await callback.message.edit_text("📋 Список всех конкурсов:", reply_markup=campaigns_list_kb(rows))

@router.callback_query(F.data.startswith("adm:open:"))
async def adm_open(callback: CallbackQuery):
    await callback.answer()
    key = callback.data.split(":", 2)[2]
    await _render_campaign_card(callback, key)

@router.callback_query(F.data.startswith("adm:on:"))
async def adm_on(callback: CallbackQuery):
    await callback.answer()
    key = callback.data.split(":", 2)[2]
    set_campaign_status(key, "active")
    await _render_campaign_card(callback, key)

@router.callback_query(F.data.startswith("adm:off:"))
async def adm_off(callback: CallbackQuery):
    await callback.answer()
    key = callback.data.split(":", 2)[2]
    set_campaign_status(key, "ended")
    await _render_campaign_card(callback, key)

@router.callback_query(F.data.startswith("adm:del:"))
async def adm_delete(callback: CallbackQuery):
    await callback.answer()
    key = callback.data.split(":", 2)[2]
    delete_campaign(key)
    await adm_list(callback)

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
async def save_winners_msg(message: Message, state: FSMContext):
    data = await state.get_data()
    key = data.get("campaign_key")
    usernames = [
        line.strip().lstrip("@")
        for line in message.text.splitlines()
        if line.strip()
    ]
    count = add_winners(key, usernames)
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
    key = message.text.strip()
    if " " in key or len(key) < 3:
        await message.answer("❌ KEY без пробелов, минимум 3 символа. Введи снова:")
        return
    await state.update_data(key=key)
    await state.set_state(CampaignCreate.amount)
    await message.answer("Теперь введи награду (число), например: 10")

@router.message(CampaignCreate.amount)
async def adm_new_amount(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Нужна награда числом > 0. Пример: 10")
        return
    await state.update_data(amount=amount)
    await state.set_state(CampaignCreate.title)
    await message.answer("И последнее — введи название конкурса (title):")

@router.message(CampaignCreate.title)
async def adm_new_title(message: Message, state: FSMContext):
    title = message.text.strip()
    data = await state.get_data()
    key = data["key"]
    amount = data["amount"]

    upsert_campaign(key, title, amount, "draft")
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
async def adm_stats_menu(callback: CallbackQuery):
    await callback.answer()

    rows = list_campaigns_latest(limit=5)
    if not rows:
        await callback.message.edit_text("Нет конкурсов", reply_markup=admin_back_kb())
        return

    total_assigned_sum = total_assigned_amount()
    claims_count_all, total_claimed_all = global_claims_stats()
    total_balances_sum = total_balances()
    active_cnt, ended_cnt, draft_cnt = campaigns_status_counts()
    unclaimed_sum = unclaimed_total_amount()

    await callback.message.edit_text(
        "📊 Полная статистика:\n\n"
        f"🎁 Всего начислено: {total_assigned_sum:.2f}⭐\n"
        f"📦 Невостребовано: {unclaimed_sum:.2f}⭐\n"
        f"💰 Всего заклеймили: {total_claimed_all:.2f}⭐\n ({claims_count_all} клеймов)\n"
        f"🏦 Всего на балансах: {total_balances_sum:.2f}⭐\n\n"
        f"🟡 Черновиков: {draft_cnt}\n"
        f"🟢 Активных конкурсов: {active_cnt}\n"
        f"🔴 Завершённых: {ended_cnt}\n\n"
        "Последние 5 конкурсов:",
        reply_markup=stats_list_kb(rows)
    )

@router.callback_query(F.data.startswith("adm:stats:"))
async def adm_stats(callback: CallbackQuery):
    await callback.answer()
    key = callback.data.split(":")[2]

    claims_count, winners_cnt, total_paid = campaign_stats(key)
    claimed = claimed_usernames(key)

    if claimed:
        claimed_text = "\n".join([f"@{u}" for u in claimed[:50]])
        if len(claimed) > 50:
            claimed_text += f"\n… и ещё {len(claimed)-50}"
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
async def adm_show_winners(callback: CallbackQuery):
    await callback.answer()
    key = callback.data.split(":")[2]

    winners = list_winners(key)
    claimed = set(claimed_usernames(key))  # кто заклеймил

    if not winners:
        text = "Победителей нет"
    else:
        lines = []
        for i, u in enumerate(winners[:50], start=1):
            mark = " ✅" if u in claimed else ""
            lines.append(f"{i}. @{u}{mark}")

        text = "\n".join(lines)

    await callback.message.edit_text(
        f"👥 Победители конкурса {key}:\n\n{text}",
        reply_markup=admin_back_kb()
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
async def winner_del_finish(message: Message, state: FSMContext):
    data = await state.get_data()
    key = data["campaign_key"]
    username = (message.text or "").strip()

    ok, msg = delete_winner_if_not_claimed(key, username)
    await state.clear()

    if ok:
        await message.answer(f"✅ Удалил {username} из победителей конкурса {key}")
    else:
        await message.answer(f"⚠️ {msg}")

@router.callback_query(F.data == "adm:top")
async def adm_top_balances(callback: CallbackQuery):
    await callback.answer()

    rows = top_users_by_balance(10)

    if not rows:
        text = "🏆 Топ-10 по балансу:\n\nПока нет пользователей с балансом ⭐️"
    else:
        lines = []
        for i, (username, balance) in enumerate(rows, start=1):
            name = f"@{username}" if username else "(без username)"
            lines.append(f"{i}. {name} — {float(balance):.2f}⭐️")
        text = "🏆 Топ-10 по балансу:\n\n" + "\n".join(lines)

    await callback.message.edit_text(text, reply_markup=admin_back_kb())

@router.callback_query(F.data == "adm:growth_png")
async def adm_growth_png(callback: CallbackQuery):
    await callback.answer()

    days = 30
    total = users_total_count()
    new_1d = users_new_since_hours(24)
    new_7d = users_new_since_days(7)
    new_30d = users_new_since_days(30)

    active_1d = users_active_since_days(1)
    active_7d = users_active_since_days(7)
    active_30d = users_active_since_days(30)

    points = users_growth_by_day(days)

    fig = plt.figure()
    ax = fig.add_subplot(111)

    if points:
        data = {d: int(cnt) for d, cnt in points}

        xs = [(date.today() - timedelta(days=days - 1 - i)).isoformat() for i in range(days)]
        ys = [data.get(d, 0) for d in xs]

        ax.bar(xs, ys)
        ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

        ax.set_ylim(bottom=0)
        ax.set_xticks(xs[::max(1, len(xs)//15)])
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
        f"📈 Рост пользователей\n\n"
        f"👥 Всего: {total}\n\n"
        f"🆕 Новые:\n"
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
