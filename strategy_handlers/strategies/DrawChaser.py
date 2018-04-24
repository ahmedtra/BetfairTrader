from betfair.constants import Side
from betfair.price import price_ticks_away, nearest_price
from structlog import get_logger

from betfair_wrapper.betfair_wrapper_api import get_api
from selection_handlers.execution import Execution


from strategy_handlers.strategy import Strategy

MAX_STAKE = 4
MIN_STAKE = 4

class DrawChaser(Strategy):
    def __init__(self, event_id, event_name = None, **params):
        super(DrawChaser, self).__init__(event_id, event_name = event_name, **params)
        get_logger().info("creating Runner_under_market", event_id = event_id)
        self.target_profit = 5000
        self.the_draw = None
        self.traded = False
        self.customer_ref = "draw"
        if "thresh_draw" in params.keys():
            self.draw_limit = nearest_price(params["thresh_draw"])
        else:
            self.draw_limit = 1.01


    def create_runner_info(self):
        get_logger().info("checking for runner under market", event_id = self.event_id)
        markets = get_api().get_markets(self.event_id, "MATCH_ODDS")
        get_logger().info("got markets", number_markets = len(markets), event_id = self.event_id)
        return get_api().get_runners(markets)

    def get_draw_bet(self):
        get_logger().info("getting draw runner")
        self.the_draw = None
        for runner in self.list_runner.values():
            if runner["selection_id"] not in self.win.keys():
                self.win[runner["selection_id"]] = 0
                self.lost[runner["selection_id"]] = 0
            if runner["runner_name"] == "The Draw":
                self.the_draw = runner["selection_id"]

    def compute_stake(self):
        get_logger().debug("computing stake", event_id = self.event_id)
        already_traded = 0
        for key in self.lost.keys():
            if key < self.the_draw:
                already_traded -= self.lost[key]

        self.current_back = self.prices[self.the_draw]["back"]
        self.current_lay = self.prices[self.the_draw]["lay"]

        self.already_traded = max(-already_traded, 0)

        self.stake = (already_traded + self.target_profit) / (self.prices[self.the_draw]["back"] - 1) * 1.2
        get_logger().info("computed stake : ",stake = self.stake, already_traded = already_traded,
                          price = self.current_back, event_id = self.event_id)

        self.stake = round(self.stake, 2)

        if self.stake < MIN_STAKE:
            self.stake = MIN_STAKE
            get_logger().info("stake inferior than minimum, setting to %f".format(MIN_STAKE),
                              stake = self.stake, price = self.current_back, event_id = self.event_id)
        elif self.stake> MAX_STAKE:
            self.stake = MAX_STAKE
            get_logger().info("stake superior than maximum, setting to %f".format(MAX_STAKE),
                              stake = self.stake, price = self.current_back, event_id = self.event_id)

        return self.stake

    def place_bet_on_the_draw(self):
        price = self.current_back
        size = self.stake
        spread = self.prices[self.the_draw]["spread"]
        selection_id = self.list_runner[self.the_draw]["selection_id"]
        market_id = self.list_runner[self.the_draw]["market_id"]

        get_logger().info("placing bet", price = price, size = size,
                          spread = spread, selection_id = selection_id,
                          market_id = market_id, event_id = self.event_id)
        if price is not None and spread is not None and spread < 20:
            price_chaser = Execution(market_id, selection_id, self.customer_ref, self.strategy_id)
            matches = price_chaser.reach_stake(price, size, Side.BACK)
            if matches is None:
                self.traded = False
                return self.traded
            self.traded = True
        else:
            get_logger().info("trade condition was not met, skipping ...", event_id = self.event_id)
            self.traded = False

        get_logger().info("trade flag", traded = self.traded, event_id = self.event_id)
        return self.traded

    def compute_profit_loss(self):
        selection_id = self.list_runner[self.the_draw]["selection_id"]
        market_id = self.list_runner[self.the_draw]["market_id"]
        pc = Execution(market_id=market_id, selection_id = selection_id, customer_order_ref= self.customer_ref, strategy_id=self.strategy_id)

        closed_market_outcome = 0
        for key in self.lost.keys():
            if key < self.the_draw:
                closed_market_outcome += self.lost[key]

        current_profit = 0
        if pc.ask_for_price():
            current_lay = pc.current_lay
            if current_lay is None:
                return None
            current_profit = (self.win[self.the_draw] + self.lost[self.the_draw] * current_lay)/current_lay

        profit = closed_market_outcome + current_profit

        return profit

    def looper(self):
        self.list_runner = self.create_runner_info()
        self.update_runner_current_price()
        if len(self.list_runner) == 0:
            get_logger().info("no runner, skipping iteration", event_id = self.event_id)
            return False

        get_logger().info("starting iteration", traded = self.traded, event_id = self.event_id)

        self.get_draw_bet()

        if self.the_draw is None:
            get_logger().info("no draw, quitting strategy", event_id = self.event_id)
            return False

        self.inplay = self.prices[self.the_draw]["inplay"]

        if not self.traded:
            self.compute_stake()

            if not self.inplay:
                self.place_passif_bet()
            else:

                if self.prices[self.the_draw]["spread"] > 20 and self.already_traded == 0:
                    get_logger().info("very wide spread, strategy not invested, quitting",
                                      spread=self.prices[self.the_draw]["spread"], event_id=self.event_id)
                    return False

                if self.prices[self.the_draw]["back"] < 2 and self.already_traded == 0:
                    get_logger().info("very low price, strategy not invested, quitting",
                                      spread=self.prices[self.the_draw]["back"], event_id=self.event_id)
                    return False

                if self.prices[self.the_draw]["back"] < self.draw_limit:
                    get_logger().info("very low price, quitting",
                                      spread=self.prices[self.the_draw]["back"], event_id=self.event_id)
                    return False


                self.place_bet_on_the_draw()

        else:
           self.pl = self.compute_profit_loss()
           get_logger().info("profit and loss", pl = self.pl, event_id = self.event_id)

        return True

    def place_passif_bet(self):
        selection_id = self.list_runner[self.the_draw]["selection_id"]
        market_id = self.list_runner[self.the_draw]["market_id"]
        size = self.stake
        if self.current_lay is None:
            price = nearest_price(max(self.current_back * 10, 200))
        else:
            price = price_ticks_away(self.current_lay, -1)

        price = max(self.draw_limit, price)
        pricer = Execution(market_id, selection_id, self.customer_ref, self.strategy_id)
        pricer.quote(price, size, Side.BACK)



