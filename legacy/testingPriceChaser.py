from common import initialize_logging
from strategy_handlers_under_goals.priceChaser import PriceChaser
from strategy_handlers_under_goals.utils import authenticate

initialize_logging("testing_price_chaser")

client = authenticate()

market_id = "1.132089559"
selection_id = 5851482

pc = PriceChaser(client, market_id, selection_id)
# pc.chasePrice(1000, 10)
orders = pc.get_betfair_matches()

print("here")