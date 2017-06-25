from strategy_handlers_under_goals import UnderGoalsChaser
from strategy_handlers_under_goals.utils import authenticate

client = authenticate()

s = UnderGoalsChaser("28251530", client)

# s.place_bet_on_most_active()
s
s.get_placed_orders()
s.get_bf_profit_and_loss()
print("here")