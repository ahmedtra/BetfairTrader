from betfair.constants import Side
from structlog import get_logger

from selection_handlers.execution import Execution
from strategy_handlers.strategy import Strategy
from strategy_handlers_market_maker.pricer import Pricer
from strategy_handlers_market_maker.utils import get_placed_orders, get_profit_and_loss, \
    get_runner, get_runner_prices

from betfair.price import price_ticks_away, ticks_difference, MIN_PRICE, MAX_PRICE

MAX_STAKE = 8
MIN_STAKE = 0

STARTING_STAKE = 4

class MarketMaker(Strategy):
    def __init__(self, market_id, client):
        super(MarketMaker, self).__init__(market_id, client)
        get_logger().info("creating MarketMaker", market_id = market_id)
        self.client = client
        self.market_id = market_id
        self.target_profit = 4
        self.current_back = 1.01
        self.current_lay = 1000
        self.stake = 0
        self.already_traded = 0
        self.traded = False
        self.list_runner = self.create_runner_info()
        self.prices = {}
        self.traded_account = []
        self.pricer = {}
        self.create_runner_info()
        self.hedge_order_back = {"side": "back", "size": 0.0, "price": self.current_back}
        self.hedge_order_lay = {"side": "lay", "size": 0.0, "price": self.current_back}
        self.selection_id = self.list_runner[list(self.list_runner.keys())[0]]["selection_id"]
        self.non_matched_orders = []
        self.matched_orders = []
        self.pricer = Execution(self.client, market_id, self.selection_id)


    def create_runner_info(self):
        get_logger().info("checking for runner for market", market_id = self.market_id)
        runners = get_runner(self.client, self.market_id)
        get_logger().info("got runners", number_markets = len(runners), market_id = self.market_id)
        return runners

    def compute_hedge(self):

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
        self.hedge_order_lay["price"] = mk_lay
        get_logger().debug("order hedging by lay",  market_id=self.market_id,
                       lay=self.current_lay, size=self.hedge_order_lay["size"])

        self.hedge_order_back["side"] = "back"
        self.hedge_order_back["size"] = min(max(round(STARTING_STAKE - (self.unhedged_position / mk_back),2), MIN_STAKE),MAX_STAKE)
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
        pc = Pricer(self.client, market_id=market_id, selection_id = selection_id)
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
        if len(self.list_runner) == 0:
            get_logger().info("no runner, skipping iteration", market_id = self.market_id)
            return False
        if len(self.list_runner) > 2:
            get_logger().info("market_id has more that 2 runners, skipping iteration", market_id=self.market_id)
            return False

        get_logger().info("starting iteration", traded = self.traded, event_id = self.market_id)

        self.compute_hedge()

        self.place_spread()

        return True

