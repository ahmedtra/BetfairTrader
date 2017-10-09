from betfair.constants import Side
from structlog import get_logger
from abc import ABC, abstractmethod

from betfair_wrapper.authenticate import get_api
from betfair_wrapper.order_utils import get_price_market_selection
from time import sleep
from betfair_wrapper.order_utils import cancel_order, place_bet
from selection_handlers.positionFetcher import positionFetcher
from selection_handlers.priceService import priceService


class Execution(positionFetcher, priceService):
    def __init__(self, market_id, selection_id, customer_order_ref = None):
        super(Execution, self).__init__(market_id, selection_id, customer_order_ref)

        self.current_orders = None
        self.current_back = None
        self.current_lay = None
        self.current_size = None
        self.status = None

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
            cancel_order(self.client, self.market_id, well_priced_orders[-1]["bet_id"], difference_position)

        elif difference_position<0:
            remaining_size = -difference_position
            get_logger().info("placing bet", current_price=self.current_back, current_size=self.current_size,
                              price=price, size=size)
            match = place_bet(self.client, price, remaining_size, side, self.market_id, self.selection_id, customer_order_ref = self.customer_order_ref)
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
                get_api().cancel_order(self.market_id, order["bet_id"])

        difference_position = well_priced_position - betting_size

        if difference_position >0:
            get_api().cancel_order(self.market_id, well_priced_orders[-1].bet_id, difference_position)

        elif difference_position<0:
            remaining_size = -difference_position
            get_logger().info("placing bet", current_price=self.current_back, current_size=self.current_size,
                              price=price, size=size)
            match = get_api().place_bet(price, remaining_size, side, self.market_id, self.selection_id)
            bet_id = match["bet_id"]
            if bet_id is None:
                get_logger().info("order refused")
                return False

            return True

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
            self.execute(self.current_back, back_hedge, Side.BACK)

    def cancel_all_pending_orders(self):
        self.get_betfair_matches()
        for order in self.unmatched_order:
            get_api().cancel_order(self.market_id, order["bet_id"])

    def compute_already_executed(self):
        sum = 0
        for match in self.matches:
            sum += match["size"]
        return sum