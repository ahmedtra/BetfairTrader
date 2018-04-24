from os.path import join

from common import initialize_logging, ROOT_DIR
from predictors.RFRPredictor import RFRPredictor
from strategy_handlers.strategies.UnderGoalsTimer import UnderGoalsTimer
from strategy_handlers.strategies_manager import strategy_manager

if __name__ == "__main__":
    initialize_logging("under_goals_2")

    time_limit = 5
    min_odds = 1.1
    max_odds = 4
    market_under_goals = 2
    min_vol = 0
    stake = 4
    number_parallel_stragies = 10
    market_countries = ["GB", "ES", "DE", "IT", "PT", "FR", "BR", "NL", "BE"]
    market_countries = None
    sm = strategy_manager(UnderGoalsTimer, event_id = None, number_threads=number_parallel_stragies,
                          timer = time_limit, market_under_goals = market_under_goals, stake = stake,
                          min_odds = min_odds, min_vol = min_vol, market_countries = market_countries)
    sm.manage_strategies()