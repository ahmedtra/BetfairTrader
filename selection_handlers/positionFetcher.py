from betfair.constants import Side
from structlog import get_logger

from betfair_wrapper.order_utils import get_placed_orders

from selection_handlers import Selection

class positionFetcher(Selection):
    def __init__(self, client, market_id, selection_id):
        super(positionFetcher, self).__init__(client, market_id, selection_id)

        self.matches = []
        self.matched_order = []
        self.unmatched_order = []
        self.win = 0
        self.lost = 0

    def get_betfair_matches(self, side = None):
        orders = get_placed_orders(self.client, market_ids=[self.market_id])
        matches = []
        non_matches = []
        market_position = 0

        for order in orders:
            if order.selection_id != self.selection_id:
                continue

            if side is not None:
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
                non_match["price"] = order.price_size.quote
                non_match["size"] = order.size_remaining
                non_match["side"] = order.side
                non_matches.append(non_match)

        self.matched_order = matches
        self.unmatched_order = non_matches

        return market_position

    def compute_unhedged_position(self):
        matched_orders = []
        matches_back = self.get_betfair_matches(Side.BACK)
        matched_orders = matched_orders + self.matched_order
        matches_lay = self.get_betfair_matches(Side.LAY)
        matched_orders = matched_orders + self.matched_order
        back_position = 0
        back_price = 0

        lay_position = 0
        lay_price = 0

        for traded in matched_orders:
            if traded["side"] == "BACK":
                back_position += traded["size"]
                back_price += traded["price"] * traded["size"]
            if traded["side"] == "LAY":
                lay_position += traded["size"]
                lay_price += traded["price"] * traded["size"]
        if back_position > 0:
            back_price = back_price / back_position
        if lay_position > 0:
            lay_price = lay_price / lay_position



        win_outcome = back_price * back_position - lay_price * lay_position
        if win_outcome == 0:
            win_outcome = self.win

        lost_outcome = - back_position + lay_position
        if lost_outcome == 0:
            lost_outcome = self.lost

        get_logger().debug("back position", market_id= self.market_id,
                           back=back_price, stake=back_position)

        get_logger().debug("lay position", market_id= self.market_id,
                           lay=back_price, stake=back_position)

        get_logger().info("win_outcome", market_id= self.market_id,
                           hedge=win_outcome)
        get_logger().info("loss_outcome", market_id= self.market_id,
                           hedge=lost_outcome)

        self.win = win_outcome
        self.lost = lost_outcome
        return win_outcome