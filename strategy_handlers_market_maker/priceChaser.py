from structlog import get_logger

from strategy_handlers_under_goals.utils import get_price_market_selection, place_bet, replace_order, get_placed_orders
from time import sleep

class PriceChaser():
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
        self.bet_fair_matches = []

    def chasePrice(self, price, size):
        get_logger().info("start chasing", price = price, size = size)
        tradable = self.ask_for_price()

        if not tradable:
            get_logger("was not able to retrieve prices", status = self.status)
            return None

        betfair_position = self.get_betfair_matches()
        get_logger().info("position_betfair", betfair_position=betfair_position)

        if betfair_position >= size:
            get_logger().info("already_placed_positions", betfair_position=betfair_position)
            return self.bet_fair_matches

        size -= betfair_position

        get_logger().info("placing initial bet", current_price = self.current_back, current_size = self.current_size, price = price, size = size)
        match = place_bet(self.client, self.current_back, size, self.market_id, self.selection_id)
        bet_id = match["bet_id"]
        already_executed = match["size"]
        if bet_id is None:
            get_logger().info("order refused")
            return None

        self.matches.append(match)

        get_logger().info("match", exucuted = already_executed, bet_id = bet_id, average_price = match["price"])

        betfair_position = self.get_betfair_matches()

        get_logger().info("position_betfair", betfair_position = betfair_position)

        while already_executed < size and betfair_position < size:
            tradable = self.ask_for_price()
            if not tradable:
                get_logger("was not able to retrieve prices", status=self.status)
                break
            get_logger().info("replacing order", current_price = self.current_back, current_size = self.current_size, price = price, size = size)
            match = replace_order(self.client, self.market_id, bet_id, self.current_back)
            bet_id = match["bet_id"]
            betfair_position = self.get_betfair_matches()
            get_logger().info("position_betfair", betfair_position=betfair_position)
            if bet_id is None:
                get_logger().info("was not able to reach position")
                break
            self.matches.append(match)
            already_executed = self.compute_already_executed()
            get_logger().info("match", already_executed = already_executed, bet_id = bet_id,
                              average_price = match["price"], betfair_position = betfair_position)

        return self.bet_fair_matches

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

    def get_betfair_matches(self):
        orders = get_placed_orders(self.client, market_ids=[self.market_id])
        matches = []
        market_position = 0
        for order in orders:
            if order.selection_id != self.selection_id:
                continue

            match = {}
            match["bet_id"] = order.bet_id
            match["price"] = order.average_price_matched
            match["size"] = order.size_matched
            market_position += order.size_matched
            matches.append(match)

        self.bet_fair_matches = matches

        return market_position



    def compute_already_executed(self):
        sum = 0
        for match in self.matches:
            sum += match["size"]
        return sum