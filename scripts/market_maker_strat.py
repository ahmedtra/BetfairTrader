from strategy_handlers.strategies_manager import strategy_manager
from strategy_handlers.strategies.marketMaker import MarketMaker
from common import initialize_logging
if __name__ == "__main__":
    initialize_logging("market_maker")
    sm = strategy_manager(MarketMaker)
    sm.manage_strategies()