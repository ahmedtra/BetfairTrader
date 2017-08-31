from structlog import get_logger

from betfair_wrapper.order_utils import get_price_market_selection, place_bet, get_placed_orders, cancel_order
from executor.execution import Execution
from time import sleep

class Pricer(Execution):

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
