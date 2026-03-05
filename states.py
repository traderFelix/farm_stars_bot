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
