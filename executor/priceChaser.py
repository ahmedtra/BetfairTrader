from structlog import get_logger

from betfair_wrapper.order_utils import place_bet, replace_order
from executor.execution import Execution

class PriceChaser(Execution):

    def execute(self, price, size, side):
        get_logger().info("start chasing", price = price, size = size)
        tradable = self.ask_for_price()

        if not tradable:
            get_logger("was not able to retrieve prices", status = self.status)
            return None

        betfair_position = self.get_betfair_matches(side)
        get_logger().info("position_betfair", betfair_position=betfair_position)

        if betfair_position >= size:
            get_logger().info("already_placed_positions", betfair_position=betfair_position)
            return self.matched_order

        size -= betfair_position

        get_logger().info("placing initial bet", current_price = self.current_back, current_size = self.current_size, price = price, size = size)
        match = place_bet(self.client, self.current_back, size, side, self.market_id, self.selection_id)
        bet_id = match["bet_id"]
        already_executed = match["size"]
        if bet_id is None:
            get_logger().info("order refused")
            return None

        self.matches.append(match)

        get_logger().info("match", exucuted = already_executed, bet_id = bet_id, average_price = match["price"])

        betfair_position = self.get_betfair_matches(side)

        get_logger().info("position_betfair", betfair_position = betfair_position)

        while already_executed < size and betfair_position < size:
            tradable = self.ask_for_price()
            if not tradable:
                get_logger("was not able to retrieve prices", status=self.status)
                break
            get_logger().info("replacing order", current_price = self.current_back, current_size = self.current_size, price = price, size = size)
            match = replace_order(self.client, self.market_id, bet_id, self.current_back)
            bet_id = match["bet_id"]
            betfair_position = self.get_betfair_matches(side)
            get_logger().info("position_betfair", betfair_position=betfair_position)
            if bet_id is None:
                get_logger().info("was not able to reach position")
                break
            self.matches.append(match)
            already_executed = self.compute_already_executed()
            get_logger().info("match", already_executed = already_executed, bet_id = bet_id,
                              average_price = match["price"], betfair_position = betfair_position)

        return self.matched_order
