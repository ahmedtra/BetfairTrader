import queue
from time import sleep

from betfair.constants import MarketSort
from betfair.models import MarketFilter
from structlog import get_logger

from betfair_wrapper.authenticate import authenticate
from common import initialize_logging
from list_team import team_list
from strategy_handlers_market_maker.strategyPlayer import MarketMakerStrategyPlayer
from strategy_handlers_market_maker.utils import client_manager
initialize_logging("MarketMaking")

class strategy_manager():
    def __init__(self):
        self.client = authenticate()
        self.type_ids = [1]
        self.queue = queue.Queue()
        self.thread_pool = {}
        self.max_threads = 1
        self.traded_markets = []
        self.client_manager = client_manager(self.client)
        self.client_manager.start()

    def retrieve_markets(self):
        get_logger().info("fetching markets")
        markets = self.client.list_market_catalogue(
            MarketFilter(event_type_ids=self.type_ids, event_types=team_list, in_play_only = False),
            sort = MarketSort.MAXIMUM_TRADED
        )
        get_logger().info("fetching all markets", number_markets = len(markets))

        return markets

    def market_generator(self):
        while True:
            markets = self.retrieve_markets()
            market = None
            while markets:
                market = markets.pop()

                if market.market_id in self.traded_markets:
                    get_logger().info("found already traded", market_name=market.market_name, market_id=market.market_id)
                    market = None
                    continue

                get_logger().info("found market to trade", market_name=market.market_name, market_id=market.market_id)
                self.traded_markets.append(market.market_id)
                break

            if market is None:
                get_logger().info("no markets, waiting ...")
                sleep(120)
            else:
                yield market

    def manage_strategies(self):

        for market in self.market_generator():

            market_id = market.market_id
            market_id = "1.132430858"
            event_id = None
            get_logger().info("creating thread for strategy", event_id = event_id, market_name = market.market_name,
                              market_id = market_id)
            self.thread_pool[market_id] = MarketMakerStrategyPlayer(self.queue, self.client, market_id)
            self.thread_pool[market_id].start()

            if len(self.thread_pool) >= self.max_threads:
                get_logger().info("strategy pool is full, block waiting ...")
                market_id = self.queue.get(True)
                get_logger().info("strategy finished, removing from the pool", market_id = market_id)
                self.thread_pool[market_id].join()
                del self.thread_pool[market_id]

        get_logger().info("stopping client manager")
        self.client_manager.stop()
        self.client_manager.join()



if __name__ == "__main__":
    sm = strategy_manager()
    sm.manage_strategies()