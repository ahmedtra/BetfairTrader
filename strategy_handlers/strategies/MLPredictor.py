
from betfair.price import nearest_price
from structlog import get_logger

from betfair_wrapper.betfair_wrapper_api import get_api
from selection_handlers.execution import Execution

from strategy_handlers.strategy import Strategy

MAX_STAKE = 20
MIN_STAKE = 0

class MLPredictor(Strategy):
    def __init__(self, event_id, event_name = None, **params):
        super(MLPredictor, self).__init__(event_id,event_name = event_name,  **params)
        get_logger().info("creating Runner_under_market", event_id = event_id)
        self.target_profit = 5000
        self.traded = False
        self.customer_ref = "ML"
        assert("predictor" in params.keys(), "no predictor given")
        self.predictor = params["predictor"]
        self.regressor = {}
        self.regressor_team_map = {}
        self.bet = None
        self.bet_selection_id = None
        if "min_odds" in params.keys():
            self.min_odds = nearest_price(params["min_odds"])
        else:
            self.min_odds = 1.01
        if "max_odds" in params.keys():
            self.max_odds = nearest_price(params["max_odds"])
        else:
            self.max_odds = 1000

    def create_runner_info(self):
        get_logger().info("checking for runner under market", event_id = self.event_id)
        markets = get_api().get_markets(self.event_id, "MATCH_ODDS")
        get_logger().info("got markets", number_markets = len(markets), event_id = self.event_id)
        return get_api().get_runners(markets)

    def get_regressors(self):
        get_logger().info("getting draw runner")

        for runner in self.prices.values():
            inplay = runner["inplay"]
            price = runner["back"] if inplay else (runner["back"] + runner["lay"]) * 0.5
            if runner["selection_id"] not in self.win.keys():
                self.win[runner["selection_id"]] = 0
                self.lost[runner["selection_id"]] = 0
            if self.split_team(runner["event_name"], 0) == runner["runner_name"]:
                self.regressor["team1"] = runner["runner_name"]
                self.regressor["1"] = price
                self.regressor["1_back"] = runner["back"]
                self.regressor_team_map["1"] = runner["selection_id"]
            elif self.split_team(runner["event_name"], 1) == runner["runner_name"]:
                self.regressor["team2"] = runner["runner_name"]
                self.regressor["2"] = price
                self.regressor["2_back"] = runner["back"]
                self.regressor_team_map["2"] = runner["selection_id"]
            elif runner["runner_name"] == "The Draw":
                self.regressor["x"] = price
                self.regressor["x_back"] = runner["back"]
                self.regressor_team_map["x"] = runner["selection_id"]

    @staticmethod
    def split_team(s, i):
        if isinstance(s, str):
            teams = s.split(" v ")
            if len(teams) > i:
                return teams[i]
            else:
                return None
        else:
            return None

    def compute_stake(self):
        get_logger().debug("computing stake", event_id = self.event_id)

        self.bet, self.stake = self.predictor.get_bet(self.regressor)
        self.bet_selection_id = self.regressor_team_map[self.bet]

        self.current_back = self.prices[self.bet_selection_id]["back"]
        self.current_lay = self.prices[self.bet_selection_id]["lay"]


        get_logger().info("computed stake : ",stake = self.stake,
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

    def compute_profit_loss(self):
        selection_id = self.list_runner[self.bet_selection_id]["selection_id"]
        market_id = self.list_runner[self.bet_selection_id]["market_id"]
        pc = Execution(market_id=market_id, selection_id = selection_id,
                       customer_order_ref= self.customer_ref, strategy_id = self.strategy_id)

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

        get_logger().info("starting iteration", traded = self.traded, event_id = self.event_id)

        self.get_regressors()

        if not self.traded:
            last_stake = self.stake
            old_bet = self.bet_selection_id
            self.compute_stake()
            if old_bet != self.bet_selection_id:
                self.cancel_all_pending_orders()

            self.inplay = self.prices[self.bet_selection_id]["inplay"]

            if self.bet_selection_id is None:
                get_logger().info("no b, quitting strategy", event_id=self.event_id)
                return False



            if not self.inplay:
                if self.stake == 0:
                    self.cancel_all_pending_orders()
                    return True
                self.passif_bet(self.bet_selection_id, self.stake, min_odds=self.min_odds, per_of_spread=0.8)
            else:
                if self.stake == 0:
                    get_logger().info("no signal at the start of the game",
                                       event_id=self.event_id)

                    return False
                if self.prices[self.bet_selection_id]["spread"] > 20 and self.already_traded == 0:
                    get_logger().info("very wide spread, strategy not invested, quitting",
                                      spread=self.prices[self.bet_selection_id]["spread"], event_id=self.event_id)
                    return False

                if self.prices[self.bet_selection_id]["back"] < self.min_odds and self.already_traded == 0:
                    get_logger().info("very low price, strategy not invested, quitting",
                                      spread=self.prices[self.bet_selection_id]["back"], event_id=self.event_id)
                    return False

                if self.prices[self.bet_selection_id]["back"] < self.min_odds:
                    get_logger().info("very low price, quitting",
                                      spread=self.prices[self.bet_selection_id]["back"], event_id=self.event_id)
                    return False

                self.traded = self.bet(self.bet_selection_id, self.stake)

        else:
           self.pl = self.compute_profit_loss()
           get_logger().info("profit and loss", pl = self.pl, event_id = self.event_id)

        return True




