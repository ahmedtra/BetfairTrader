from betfair.constants import Side
from structlog import get_logger
from abc import ABC, abstractmethod
from betfair_wrapper.order_utils import get_price_market_selection
from time import sleep

from executor.positionFetcher import positionFetcher


class Execution(positionFetcher):
    def __init__(self, client, market_id, selection_id):
        super(Execution, self).__init__(client, market_id, selection_id)
        get_logger().info("creating exection", market_id=market_id, selection_id=selection_id)
        self.current_orders = None
        self.current_back = None
        self.current_lay = None
        self.current_size = None
        self.status = None

        self.positionFetcher = positionFetcher(client, market_id, selection_id)

    @abstractmethod
    def execute(self, price, size, side):
        pass

    def ask_for_price(self):
        self.current_back, self.current_lay, self.current_size, self.status, self.current_orders = \
            get_price_market_selection(self.client, self.market_id, self.selection_id)
        while self.status == "SUSPENDED":
            sleep(10)
            self.current_back, self.lay, self.current_size, self.status, self.current_orders = \
                get_price_market_selection(self.client, self.market_id, self.selection_id)

        if self.status == "ACTIVE":
            return True
        else:
            return False

    def cashout(self, percentage = 1.0):
        unhedged_pos = self.compute_unhedged_position()
        self.ask_for_price()
        if unhedged_pos > 0:
            lay_hedge = unhedged_pos / self.current_lay
            lay_hedge = round(lay_hedge, 2)
            self.execute(self.current_lay, lay_hedge, Side.LAY)
        elif unhedged_pos < 0:
            back_hedge = - unhedged_pos / self.current_back
            back_hedge = round(back_hedge, 2)
            self.execute(self.current_lay, back_hedge, Side.LAY)

    def compute_already_executed(self):
        sum = 0
        for match in self.matches:
            sum += match["size"]
        return sum