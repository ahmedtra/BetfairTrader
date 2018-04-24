from betfair.constants import Side
from betfair.price import price_ticks_away, nearest_price
from structlog import get_logger

from betfair_wrapper.betfair_wrapper_api import get_api
from selection_handlers.execution import Execution
from selection_handlers.positionFetcher import positionFetcher
from strategy_handlers.strategy import Strategy

from datetime import datetime

MAX_STAKE = 4
MIN_STAKE = 4

class UnderGoalsTimer(Strategy):
    def __init__(self, event_id, event_name = None, **params):
        super(UnderGoalsTimer, self).__init__(event_id, event_name=event_name, **params)
        self.target_profit = 5000
        self.event_id = event_id
        self.traded = False
        self.list_runner = self.create_runner_info()
        self.traded_account = []
        self.customer_ref = "UGT"
        self.inplay = False
        self.elapsed_time = 0
        self.total_matched = 0
        self.inplay_start = None
        self.bet_selection_id = None
        if "timer" in params.keys():
            self.timer = params["timer"]
        else:
            self.timer = 15
        if "market_under_goals" in params.keys():
            self.market = params["market_under_goals"]
        else:
            self.market = 2
        if "min_vol" in params.keys():
            self.min_vol = params["min_vol"]
        else:
            self.min_vol = 5000

        self.min_odds = 1.1

    def create_runner_info(self):
        get_logger().info("checking for runner under market", event_id = self.event_id)
        markets = get_api().get_markets(self.event_id, text_query="OVER_UNDER_25")
        get_logger().info("got markets", number_markets = len(markets), event_id = self.event_id)
        return get_api().get_runners(markets)

    def update_states(self):
        get_logger().info("getting runner")
        total_matched = 0
        for runner in self.prices.values():
            self.inplay = runner["inplay"]
            price = runner["back"] if self.inplay else (runner["back"] + runner["lay"]) * 0.5
            total_matched += runner["total_matched"]
            if runner["selection_id"] not in self.win.keys():
                self.win[runner["selection_id"]] = 0
                self.lost[runner["selection_id"]] = 0
            if runner["runner_name"] == "Under {}.5 Goals".format(self.market):
                self.bet_selection_id = runner["selection_id"]
                self.state.update_state("back", runner["back"])
                self.current_back = runner["back"]

                self.state.update_state("lay", runner["lay"])
                self.current_lay = runner["lay"]
                self.state.update_state("mid", price)
                self.state.update_state("inplay", self.inplay)

                if self.inplay_start is None:
                    if self.inplay:
                        self.inplay_start = datetime.now()
                    self.elapsed_time = 0
                else:
                    self.elapsed_time = (datetime.now() - self.inplay_start).seconds / 60
                self.state.update_state("elapsed_time", self.elapsed_time)
                self.state.update_state("inplay_start", self.inplay_start)
        self.total_matched = total_matched
        self.state.update_state("total_matched", runner["total_matched"])

    def compute_stake(self):
        get_logger().debug("computing stake", event_id = self.event_id)
        already_traded = 0
        for key in self.lost.keys():
            if key < self.bet_selection_id:
                already_traded -= self.lost[key]

        self.already_traded = max(-already_traded, 0)

        get_logger().info("computed stake : ",stake = self.stake, already_traded = already_traded,
                          price = self.current_back, event_id = self.event_id)

        if self.elapsed_time < self.timer:
            self.stake = 2
        else:
            self.stake = 0

        return self.stake

    def compute_profit_loss(self):

        selection_id = self.list_runner[self.bet_selection_id]["selection_id"]
        market_id = self.list_runner[self.bet_selection_id]["market_id"]
        pc = Execution(market_id=market_id, selection_id = selection_id)

        closed_market_outcome = 0
        for key in self.lost.keys():
            if key < self.bet_selection_id:
                closed_market_outcome += self.lost[key]

        current_profit = 0
        if pc.ask_for_price():
            current_lay = pc.current_lay
            if current_lay is None:
                return None
            current_profit = (self.win[self.bet_selection_id] + self.lost[self.bet_selection_id] * current_lay)/current_lay

        profit = closed_market_outcome + current_profit

        return profit


    def looper(self):
        self.list_runner = self.create_runner_info()
        self.update_runner_current_price()
        
        if len(self.list_runner) == 0:
            get_logger().info("no runner, skipping iteration", event_id = self.event_id)
            return False

        self.update_states()

        if self.total_matched < self.min_vol:
            get_logger().info("not enough liquidity at the start, waiting", event_id=self.event_id)
            return False

        self.compute_stake()

        self.traded = self.compare_pos_stake(self.bet_selection_id, self.stake)

        if not self.traded:

            if self.inplay:
                get_logger().info("game started without taker, exit", event_id=self.event_id)
                return False

            else:
                if self.stake == 0:
                    self.cancel_all_pending_orders()
                    return True
                self.passif_bet(self.bet_selection_id, self.stake, min_odds=self.min_odds, per_of_spread=0.8)
        else:
                self.executed_price = self.get_average_executed_price(self.bet_selection_id)
                get_logger().info("no signal at the start of the game",
                                       event_id=self.event_id)

                self.exit = self.compare_pos_stake(self.bet_selection_id, self.stake, Side.LAY)

                if not self.exit:
                    self.passif_bet()
                if self.prices[self.bet_selection_id]["spread"] > 20 and self.already_traded == 0:
                    get_logger().info("very wide spread, strategy not invested, quitting",
                                      spread=self.prices[self.bet_selection_id]["spread"], event_id=self.event_id)
                    return False


                self.bet(self.bet_selection_id, self.stake)


        self.pl = self.compute_profit_loss()
        get_logger().info("profit and loss", pl=self.pl, event_id=self.event_id)

        self.state.update_state("traded", self.traded)
        self.state.update_state("inplay", self.inplay)

        return True

