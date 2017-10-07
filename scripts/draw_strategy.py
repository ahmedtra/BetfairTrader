from strategy_handlers.strategies.DrawChaser import DrawChaser
from strategy_handlers.strategies_manager import strategy_manager
from strategy_handlers.strategies.marketMaker import MarketMaker
from common import initialize_logging
if __name__ == "__main__":
    initialize_logging("draw_better")
    thresh_draw = 4.5
    sm = strategy_manager(DrawChaser, number_threads=10, thresh_draw = thresh_draw)
    sm.manage_strategies()