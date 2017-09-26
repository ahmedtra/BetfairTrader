
from structlog import get_logger


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

    @abstractmethod
    def looper(self):
        pass



