from betfair.constants import Side
from betfair.price import price_ticks_away, nearest_price
from structlog import get_logger
from datetime import datetime

from betfair_wrapper.utils import get_runner_prices

from abc import ABC, abstractmethod

from selection_handlers.execution import Execution

MAX_STAKE = 4
MIN_STAKE = 4

class Strategy(ABC):
    def __init__(self, event_id, client, **params):
        get_logger().info("creating strategy", event_id = event_id)
        self.client = client
        self.customer_ref = None
        self.current_back = None
        self.current_lay = None
        self.event_id = event_id
        self.stake = 0
        self.pl = 0
        self.already_traded = 0
        self.list_runner = self.create_runner_info()
        self.prices = {}
        self.win = {}
        self.lost = {}
        self.inplay = False
        self.params = params
        self.customer_ref_id = 0

    def generate_oder_id(self, selection_id):
        time = datetime.now().strftime("%y%m%d%H%M%S")
        if self.customer_ref is None:
            return None
        ref = self.customer_ref + "_" + str(self.customer_ref_id) + str(selection_id) + time
        self.customer_ref_id =self.customer_ref_id +1
        return ref

    @abstractmethod
    def create_runner_info(self):
        pass

    def update_runner_current_price(self):
        get_logger().info("retriving prices", event_id = self.event_id)
        self.prices = get_runner_prices(self.client, self.list_runner)
        for p in self.prices.values():
            if p["lay"] is not None and p["back"] is not None:
                p["spread"] = 2 * (p["lay"] - p["back"]) / (p["lay"] + p["back"]) * 100
            else:
                p["spread"] = None
        get_logger().info("updated the prices", event_id = self.event_id)
        return self.prices

    def cancel_all_pending_orders(self, selection_id = None, market_id = None):
        if selection_id is not None and market_id is not None:
            executioner = Execution(self.client, market_id, selection_id, self.customer_ref)
            executioner.cancel_all_pending_orders()
            return

        for runner in self.list_runner.values():
            market_id = runner["market_id"]
            selection_id = runner["selection_id"]
            executioner = Execution(self.client, market_id, selection_id, self.customer_ref)
            executioner.cancel_all_pending_orders()

    def liquidate(self, selection_id = None, market_id = None):
        if selection_id is not None and market_id is not None:
            executioner = Execution(self.client, market_id, selection_id, self.customer_ref)
            executioner.cashout()
            return

        for runner in self.list_runner.values():
            market_id = runner["market_id"]
            selection_id = runner["selection_id"]
            executioner = Execution(self.client, market_id, selection_id, self.customer_ref)
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
        ref_order = self.generate_oder_id(selection_id)
        pricer = Execution(self.client, market_id, selection_id, ref_order)
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
            ref = self.generate_oder_id(selection_id)
            price_chaser = Execution(self.client, market_id, selection_id, ref)
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



