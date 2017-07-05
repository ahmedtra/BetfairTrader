from structlog import get_logger

from strategy_handlers_market_maker.pricer import Pricer
from strategy_handlers_market_maker.utils import get_placed_orders, get_profit_and_loss, \
    get_runner, get_runner_prices

from betfair.price import price_ticks_away, ticks_difference

MAX_STAKE = 8
MIN_STAKE = 0

STARTING_STAKE = 4

class MarketMaker():
    def __init__(self, market_id, client):
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

        self.pricer_back = Pricer(self.client, market_id, self.selection_id)
        self.pricer_lay = Pricer(self.client, market_id, self.selection_id)

    def create_runner_info(self):
        get_logger().info("checking for runner for market", market_id = self.market_id)
        runners = get_runner(self.client, self.market_id)
        get_logger().info("got runners", number_markets = len(runners), market_id = self.market_id)
        return runners

    def update_runner_current_price(self):
        get_logger().info("retriving prices", market_id = self.market_id)
        self.prices = get_runner_prices(self.client, self.market_id, self.list_runner)
        for p in self.prices.values():
            if p["lay"] is not None and p["back"] is not None:
                p["spread"] = 2 * (p["lay"] - p["back"]) / (p["lay"] + p["back"]) * 100
            else:
                p["spread"] = None
        get_logger().info("updated the prices", market_id = self.market_id)
        self.current_back = self.prices[self.selection_id]["back"]
        self.current_lay = self.prices[self.selection_id]["lay"]
        return self.prices

    def compute_hedge(self):
        get_logger().debug("computing hedge", market_id = self.market_id)

        back_position = 0
        back_price = 0

        lay_position = 0
        lay_price = 0

        for traded in self.traded_account:
            if traded["side"] == "back":
                back_position += traded["size"]
                back_price += traded["price"] * traded["size"]
            if traded["side"] == "lay":
                lay_position += traded["size"]
                lay_price += traded["price"] * traded["size"]

        self.unhedged_position = back_price * back_position - lay_price * lay_position

        get_logger().debug("back position", market_id = self.market_id,
                           back = back_price, stake = back_position)

        get_logger().debug("lay position",  market_id = self.market_id,
                           lay = back_price, stake = back_position)

        get_logger().debug("hedge", market_id=self.market_id,
                           hedge = self.unhedged_position)


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

        executed_back = self.pricer_back.Price(price_back, size_back, "back")
        executed_lay = self.pricer_lay.Price(price_lay, size_lay, "lay")

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

    def get_matches(self):
        self.pricer_back.get_betfair_matches("BACK")
        self.pricer_lay.get_betfair_matches("LAY")
        self.traded_account = self.pricer_back.matched_order + self.pricer_lay.matched_order

    def get_placed_orders(self):
        market_ids = [m["market_id"] for m in self.list_runner.values()]
        get_placed_orders(self.client, market_ids=market_ids)

    def get_bf_profit_and_loss(self):
        market_ids = [m["market_id"] for m in self.list_runner.values()]
        get_profit_and_loss(self.client, market_ids=market_ids)

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

        self.get_matches()

        self.compute_hedge()

        self.place_spread()

        return True

