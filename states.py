from aiogram.fsm.state import StatesGroup, State

class CampaignCreate(StatesGroup):
    key = State()
    amount = State()
    title = State()

class AddWinners(StatesGroup):
    usernames = State()

class DeleteWinner(StatesGroup):
    username = State()

class UserLookup(StatesGroup):
    user = State()

class AdminAdjust(StatesGroup):
    amount = State()

class WithdrawCreate(StatesGroup):
    amount = State()
    details = State()
    fee_payment = State()

class AdminRefundFee(StatesGroup):
    waiting_manual_data = State()

class TaskChannelCreate(StatesGroup):
    chat_id = State()
    total_bought_views = State()
    views_per_post = State()

class TaskChannelEdit(StatesGroup):
    total_bought_views = State()
    views_per_post = State()