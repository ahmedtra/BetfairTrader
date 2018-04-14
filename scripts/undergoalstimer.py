from os.path import join

from common import initialize_logging, ROOT_DIR
from predictors.RFRPredictor import RFRPredictor
from strategy_handlers.strategies.UnderGoalsTimer import UnderGoalsTimer
from strategy_handlers.strategies_manager import strategy_manager

if __name__ == "__main__":
    initialize_logging("under_goals_2")

    time_limit = 2
    min_odds = 1.1
    max_odds = 4
    market_under_goals = 2
    stake = 4
    number_parallel_stragies = 1

    sm = strategy_manager(UnderGoalsTimer, event_id = 28652792,number_threads=number_parallel_stragies,
                          timer = time_limit,
                          market_under_goals = market_under_goals, stake = stake, min_odds = min_odds)
    sm.manage_strategies()