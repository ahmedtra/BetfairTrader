from betfair.constants import Side
from structlog import get_logger

from betfair_wrapper.betfair_wrapper_api import get_api
from selection_handlers.execution import Execution
from strategy_handlers.strategy import Strategy


from betfair.price import price_ticks_away, ticks_difference, MIN_PRICE, MAX_PRICE

MAX_STAKE = 8
MIN_STAKE = 0

STARTING_STAKE = 4

class MarketMaker(Strategy):
    def __init__(self, event_id, event_name = None):
        super(MarketMaker, self).__init__(event_id, event_name = event_name)
        get_logger().info("creating MarketMaker", event_id = event_id)
        self.target_profit = 4
        self.traded = False
        self.hedge_order_back = {"side": "back", "size": 0.0, "price": self.current_back}
        self.hedge_order_lay = {"side": "lay", "size": 0.0, "price": self.current_back}
        self.selection_id = self.list_runner[list(self.list_runner.keys())[0]]["selection_id"]
        self.market_id = self.list_runner[list(self.list_runner.keys())[0]]["market_id"]
        self.non_matched_orders = []
        self.matched_orders = []
        self.customer_ref = "market_maker"
        self.pricer = Execution(self.market_id, self.selection_id, self.customer_ref, self.strategy_id)


    def create_runner_info(self):
        get_logger().info("checking for runner for market", event_id = self.event_id)
        markets = get_api().get_markets(self.event_id, "MATCH_ODDS")
        runners = get_api().get_runners(markets)
        get_logger().info("got runners", number_markets = len(runners), event_id = self.event_id)
        return runners

    def compute_hedge(self):

        self.current_back = self.prices[self.selection_id]["back"]
        self.current_lay = self.prices[self.selection_id]["lay"]

        self.unhedged_position = self.pricer.compute_unhedged_position()

        if self.current_back is None:
            self.current_back = MIN_PRICE
        if self.current_lay is None:
            self.current_lay = MAX_PRICE

        td = ticks_difference(self.current_back, self.current_lay)

        if td <= 2:
            get_logger().debug("very tight market, sitting at the current odds", market_id=self.market_id,
                               tick_difference = td, lay = self.current_lay, back = self.current_back)
            mk_back = self.current_lay
            mk_lay = self.current_back
        else:
            mk_back = price_ticks_away(self.current_lay, -1)
            mk_lay = price_ticks_away(self.current_back, 1)
        self.hedge_order_lay["side"] = "lay"
        self.hedge_order_lay["size"] = min(max(round(STARTING_STAKE +( self.unhedged_position / mk_lay),2), MIN_STAKE),MAX_STAKE)
        self.hedge_order_lay["size"] += self.pricer.position_lay
        self.hedge_order_lay["price"] = mk_lay
        get_logger().debug("order hedging by lay",  market_id=self.market_id,
                       lay=self.current_lay, size=self.hedge_order_lay["size"])

        self.hedge_order_back["side"] = "back"
        self.hedge_order_back["size"] = min(max(round(STARTING_STAKE - (self.unhedged_position / mk_back),2), MIN_STAKE),MAX_STAKE)
        self.hedge_order_back["size"] += self.pricer.position_back
        self.hedge_order_back["price"] = mk_back

        get_logger().debug("order hedging by back",  market_id=self.market_id,
                       back=self.current_back, size=self.hedge_order_back["size"])



    def place_spread(self):
        price_back = self.hedge_order_back["price"]
        price_lay = self.hedge_order_lay["price"]
        size_back = self.hedge_order_back["size"]
        size_lay = self.hedge_order_lay["size"]
        selection_id = self.selection_id
        market_id = self.market_id

        get_logger().info("placing bet", price_back = price_back, price_lay = price_lay,
                          size_back = size_back , size_lay = size_lay,
                          selection_id = selection_id,
                          market_id = market_id)

        executed_back = self.pricer.quote(price_back, size_back, Side.BACK)
        executed_lay = self.pricer.quote(price_lay, size_lay, Side.LAY)

        get_logger().info("trade flag", traded = self.traded, market_id = self.market_id)
        return self.traded


    def compute_profit_loss(self):

        selection_id = self.list_runner[0]["selection_id"]
        market_id = self.list_runner[0]["market_id"]
        pc = Pricer(market_id=market_id, selection_id = selection_id)
        matches = pc.get_betfair_matches("back")

        profit = 0

        if pc.ask_for_price():
            current_lay = pc.current_lay
            if current_lay is None:
                return None
            for match in pc.matched_order:
                back_match = match["price"]
                size_match = match["size"]
                pl = size_match * (back_match - current_lay) / current_lay
                profit += pl

        return profit

    def looper(self):
        self.list_runner = self.create_runner_info()
        self.update_runner_current_price()
        self.get_prefered_runner()

        self.liquidate_non_active()

        if len(self.list_runner) == 0:
            get_logger().info("no runner, skipping iteration", market_id = self.market_id)
            return False
        if len(self.list_runner) > 3:
            get_logger().info("market_id has more that 2 runners, skipping iteration", market_id=self.market_id)
            return False

        get_logger().info("starting iteration", traded = self.traded, event_id = self.market_id)

        self.compute_hedge()

        self.place_spread()

        return True

    def get_prefered_runner(self):
        selection_id = self.selection_id
        prev_average_price = self.list_runner[selection_id]["back"] * 0.5 + self.list_runner[selection_id]["lay"] * 0.5
        for runner in self.list_runner.values():
            average_price = runner["back"] * 0.5 + runner["lay"] * 0.5
            if average_price < prev_average_price - 0.5:
                selection_id = runner["selection_id"]
                prev_average_price = average_price

        if selection_id != self.selection_id:
            self.cancel_all_pending_orders()
            self.liquidate()
            self.selection_id = selection_id
            self.pricer.set_runner(self.market_id, self.selection_id)

    def liquidate_non_active(self):
        for runner in self.list_runner.values():
            selection_id = runner["selection_id"]
            market_id = runner["market_id"]
            if selection_id != self.selection_id:
                self.cancel_all_pending_orders(selection_id, market_id)
                self.liquidate(selection_id, market_id)



