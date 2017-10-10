from betfair.constants import Side
from betfair.price import price_ticks_away, nearest_price
from structlog import get_logger
from datetime import datetime

from betfair_wrapper.betfair_wrapper_api import get_api
from abc import ABC, abstractmethod

from data_betfair.query import DBQuery
from selection_handlers.execution import Execution

MAX_STAKE = 4
MIN_STAKE = 4

class Strategy(ABC):
    def __init__(self, event_id, event_name = None, **params):
        self.strategy_id = None
        get_logger().info("creating strategy", event_id = event_id)
        self.customer_ref = None
        self.current_back = None
        self.current_lay = None
        self.event_id = event_id
        self.event_name = event_name
        self.stake = 0
        self.pl = 0
        self.already_traded = 0
        self.list_runner = self.create_runner_info()
        self.prices = {}
        self.win = {}
        self.lost = {}
        self.inplay = False
        self.params = params
        self.sqldb = DBQuery()

    @abstractmethod
    def create_runner_info(self):
        pass

    def update_runner_current_price(self):
        get_logger().info("retriving prices", event_id = self.event_id)
        self.prices = get_api().get_runner_prices(self.list_runner)
        for p in self.prices.values():
            if p["lay"] is not None and p["back"] is not None:
                p["spread"] = 2 * (p["lay"] - p["back"]) / (p["lay"] + p["back"]) * 100
            else:
                p["spread"] = None
        get_logger().info("updated the prices", event_id = self.event_id)
        return self.prices

    def cancel_all_pending_orders(self, selection_id = None, market_id = None):
        get_logger().info("cancelling all orders", event_id = self.event_id)
        if selection_id is not None and market_id is not None:
            executioner = Execution(market_id, selection_id, self.customer_ref, self.strategy_id)
            executioner.cancel_all_pending_orders()
            return

        for runner in self.list_runner.values():
            market_id = runner["market_id"]
            selection_id = runner["selection_id"]
            executioner = Execution(market_id, selection_id, self.customer_ref, self.strategy_id)
            executioner.cancel_all_pending_orders()

    def liquidate(self, selection_id = None, market_id = None):
        get_logger().info("liquidating all positions", event_id=self.event_id)
        if selection_id is not None and market_id is not None:
            executioner = Execution(market_id, selection_id, self.customer_ref, self.strategy_id)
            executioner.cashout()
            return

        for runner in self.list_runner.values():
            market_id = runner["market_id"]
            selection_id = runner["selection_id"]
            executioner = Execution(market_id, selection_id, self.customer_ref, self.strategy_id)
            executioner.cashout()

    def passif_bet(self,  selection_id, stake, per_of_spread = 1.0, max_odds =200, min_odds=1.01, odds_multip_if_no_spread = 10):
        selection_id = self.list_runner[selection_id]["selection_id"]
        market_id = self.list_runner[selection_id]["market_id"]
        size = stake
        if self.current_lay is None:
            price = nearest_price(max(self.current_back * odds_multip_if_no_spread, max_odds))
        else:
            price = price_ticks_away(self.current_lay, -1) * (per_of_spread) + self.current_back * (1-per_of_spread)
            price = nearest_price(price)

        price = max(min_odds, price)
        pricer = Execution(market_id, selection_id, self.customer_ref, self.strategy_id)
        pricer.quote(price, size, Side.BACK)

    def bet(self, selection_id, stake, spread_condition=20):
        price = self.prices[selection_id]["back"]
        size = stake
        spread = self.prices[selection_id]["spread"]
        selection_id = self.list_runner[selection_id]["selection_id"]
        market_id = self.list_runner[selection_id]["market_id"]

        get_logger().info("placing bet", price=price, size=size,
                          spread=spread, selection_id=selection_id,
                          market_id=market_id, event_id=self.event_id)
        if price is not None and spread is not None and spread < spread_condition:
            price_chaser = Execution(market_id, selection_id, self.customer_ref, self.strategy_id)
            matches = price_chaser.execute(price, size, Side.BACK)
            if matches is None:
                traded = False
                return traded
            traded = True
        else:
            get_logger().info("trade condition was not met, skipping ...", event_id=self.event_id)
            traded = False

        get_logger().info("trade flag", traded=traded, event_id=self.event_id)
        return traded

    @abstractmethod
    def looper(self):
        pass

    def add_strategy(self, status):
        strategy = self.sqldb.add_strategy(self.customer_ref, self.event_id, self.event_name, status)
        self.strategy_id = strategy.id
        self.sqldb.commit_changes()


