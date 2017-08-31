from betfair.constants import Side
from structlog import get_logger
from abc import ABC, abstractmethod
from betfair_wrapper.order_utils import get_price_market_selection
from time import sleep
from betfair_wrapper.order_utils import cancel_order, place_bet
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

    def quote(self, price, size, side):
        tradable = self.ask_for_price()

        if not tradable:
            get_logger("was not able to retrieve prices", status = self.status)
            return False

        betfair_position = self.get_betfair_matches(side)
        get_logger().info("position_betfair", betfair_position=betfair_position)

        betting_size = max(size - betfair_position, 0)
        well_priced_orders = []
        well_priced_position = 0
        for order in self.unmatched_order:
            if order["price"] == price and well_priced_position < betting_size:
                well_priced_orders.append(order)
                well_priced_position += order["size"]
            else:
                cancel_order(self.client, self.market_id, order["bet_id"])

        difference_position = well_priced_position - betting_size

        if difference_position >0:
            cancel_order(self.client, self.market_id, well_priced_orders[-1].bet_id, difference_position)

        elif difference_position<0:
            remaining_size = -difference_position
            get_logger().info("placing bet", current_price=self.current_back, current_size=self.current_size,
                              price=price, size=size)
            match = place_bet(self.client, price, remaining_size, side, self.market_id, self.selection_id)
            bet_id = match["bet_id"]
            if bet_id is None:
                get_logger().info("order refused")
                return False

            return True


    def execute(self, price, size, side):
        tradable = self.ask_for_price()

        if not tradable:
            get_logger("was not able to retrieve prices", status = self.status)
            return False

        betfair_position = self.get_betfair_matches(side)
        get_logger().info("position_betfair", betfair_position=betfair_position)

        betting_size = max(size - betfair_position, 0)
        well_priced_orders = []
        well_priced_position = 0
        for order in self.unmatched_order:
            if order["price"] == price and well_priced_position < betting_size:
                well_priced_orders.append(order)
                well_priced_position += order["size"]
            else:
                cancel_order(self.client, self.market_id, order["bet_id"])

        difference_position = well_priced_position - betting_size

        if difference_position >0:
            cancel_order(self.client, self.market_id, well_priced_orders[-1].bet_id, difference_position)

        elif difference_position<0:
            remaining_size = -difference_position
            get_logger().info("placing bet", current_price=self.current_back, current_size=self.current_size,
                              price=price, size=size)
            match = place_bet(self.client, price, remaining_size, side, self.market_id, self.selection_id)
            bet_id = match["bet_id"]
            if bet_id is None:
                get_logger().info("order refused")
                return False

            return True

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

    def cancel_all_pending_orders(self):
        self.get_betfair_matches()
        for order in self.unmatched_order:
                cancel_order(self.client, self.market_id, order["bet_id"])

    def compute_already_executed(self):
        sum = 0
        for match in self.matches:
            sum += match["size"]
        return sum