from structlog import get_logger

from strategy_handlers_market_maker.utils import get_price_market_selection, place_bet, replace_order, get_placed_orders, cancel_order
from time import sleep

class Pricer():
    def __init__(self, client, market_id, selection_id):
        get_logger().info("creating Price chaser", market_id = market_id, selection_id = selection_id)
        self.client = client
        self.market_id = market_id
        self.selection_id = selection_id
        self.matches = []
        self.current_orders = None
        self.current_back = None
        self.current_lay = None
        self.current_size = None
        self.status = None
        self.matched_order = []
        self.unmatched_order = []

    def Price(self, price, size, side):
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
                                get_price_market_selection(self.client, self.market_id,self.selection_id)
        while self.status == "SUSPENDED":
            sleep(10)
            self.current_back, self.lay, self.current_size, self.status, self.current_orders = \
                                get_price_market_selection(self.client,self.market_id,self.selection_id)

        if self.status == "ACTIVE":
            return True
        else:
            return False

    def get_betfair_matches(self, side):
        orders = get_placed_orders(self.client, market_ids=[self.market_id])
        matches = []
        non_matches = []
        market_position = 0

        for order in orders:
            if order.selection_id != self.selection_id:
                continue
            if order.side != side.name:
                continue

            match = {}
            non_match = {}

            match["bet_id"] = order.bet_id
            match["price"] = order.average_price_matched
            match["size"] = order.size_matched
            match["side"] = order.side
            market_position += order.size_matched
            matches.append(match)

            if order.status == "EXECUTABLE":
                non_match["bet_id"] = order.bet_id
                non_match["price"] = order.price_size.price
                non_match["size"] = order.size_remaining
                non_match["side"] = order.side
                non_matches.append(non_match)

        self.matched_order = matches
        self.unmatched_order = non_matches

        return market_position

    def compute_already_executed(self):
        sum = 0
        for match in self.matches:
            sum += match["size"]
        return sum