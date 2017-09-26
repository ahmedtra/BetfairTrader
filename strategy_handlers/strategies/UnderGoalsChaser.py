from betfair.constants import Side
from betfair.price import price_ticks_away, nearest_price
from structlog import get_logger

from selection_handlers.execution import Execution
from strategy_handlers.strategy import Strategy

from betfair_wrapper.utils import get_markets, get_runner_under


MAX_STAKE = 4
MIN_STAKE = 4

class Runners_Under_Market(Strategy):
    def __init__(self, event_id, client):
        super(Runners_Under_Market, self).__init__(event_id, client)
        self.target_profit = 5000
        self.event_id = event_id
        self.traded = False
        self.list_runner = self.create_runner_info()
        self.win = {i:0 for i in range(9)}
        self.lost = {i:0 for i in range(9)}
        self.traded_account = []

    def find_most_active(self):
        most_active = 10
        most_active_updated = False

        for i in self.prices.keys():
            if i < most_active:
                most_active = i
        if self.active < most_active:
            most_active_updated = True
            self.traded = False
        self.active = most_active

        if self.active == 10:
            return most_active_updated

        self.current_back = self.prices[most_active]["back"]
        self.current_lay = self.prices[most_active]["lay"]
        get_logger().info("computed active bet", active = most_active, price = self.current_back, event_id = self.event_id)
        print("price "+str(self.current_back))
        return most_active_updated

    def create_runner_info(self):
        get_logger().info("checking for runner under market", event_id = self.event_id)
        markets = get_markets(self.client, self.event_id, text_query="OVER_UNDER_*5")
        get_logger().info("got markets", number_markets = len(markets), event_id = self.event_id)
        return get_runner_under(markets)

    def compute_stake(self):
        get_logger().debug("computing stake", event_id = self.event_id)
        already_traded = 0
        for key in self.lost.keys():
            if key < self.active:
                already_traded -= self.lost[key]

        self.already_traded = max(-already_traded, 0)

        self.stake = (already_traded + self.target_profit) / (self.prices[self.active]["back"] - 1) * 1.2
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

    def place_bet_on_most_active(self):
        price = self.current_back
        size = self.stake
        spread = self.prices[self.active]["spread"]
        selection_id = self.list_runner[self.active]["selection_id"]
        market_id = self.list_runner[self.active]["market_id"]

        get_logger().info("placing bet", price = price, size = size,
                          spread = spread, selection_id = selection_id,
                          market_id = market_id, event_id = self.event_id)
        if price is not None and spread is not None and spread < 20:
            price_chaser = Execution(self.client, market_id, selection_id)
            matches = price_chaser.execute(price, size, Side.BACK)
            if matches is None:
                self.traded = False
                return self.traded
            self.traded_account.extend(matches)
            self.traded = True
        else:
            get_logger().info("trade condition was not met, skipping ...", event_id = self.event_id)
            self.traded = False

        get_logger().info("trade flag", traded = self.traded, event_id = self.event_id)
        return self.traded

    def compute_profit_loss(self):

        selection_id = self.list_runner[self.active]["selection_id"]
        market_id = self.list_runner[self.active]["market_id"]
        pc = Execution(self.client, market_id=market_id, selection_id = selection_id)

        closed_market_outcome = 0
        for key in self.lost.keys():
            if key < self.active:
                closed_market_outcome += self.lost[key]

        current_profit = 0
        if pc.ask_for_price():
            current_lay = pc.current_lay
            if current_lay is None:
                return None
            current_profit = (self.win[self.active] + self.lost[self.active] * current_lay)/current_lay

        profit = closed_market_outcome + current_profit

        return profit

    def get_traded_amount(self):
        already_traded = 0
        for trades in self.traded_account:
            already_traded += trades["size"]

    def looper(self):
        self.list_runner = self.create_runner_info()
        self.update_runner_current_price()
        if len(self.list_runner) == 0:
            get_logger().info("no runner, skipping iteration", event_id = self.event_id)
            return False

        get_logger().info("starting iteration", traded = self.traded, event_id = self.event_id)

        self.find_most_active()

        if self.active == 10:
            get_logger().info("no active bet, quitting strategy", event_id = self.event_id)
            return False

        self.inplay = self.prices[self.active]["inplay"]

        if not self.traded:
            self.compute_stake()

            if not self.inplay:
                self.place_passif_bet()
            else:

                if self.prices[self.active]["spread"] > 20 and self.already_traded == 0:
                    get_logger().info("very wide spread, strategy not invested, quitting",
                                      spread=self.prices[self.active]["spread"], event_id=self.event_id)
                    return False

                if self.prices[self.active]["back"] < 2 and self.already_traded == 0:
                    get_logger().info("very low price, strategy not invested, quitting",
                                      spread=self.prices[self.active]["back"], event_id=self.event_id)
                    return False

                if self.prices[self.active]["back"] < 1.2:
                    get_logger().info("very low price, taking loss, quitting",
                                      spread=self.prices[self.active]["back"], event_id=self.event_id)
                    return False


                self.place_bet_on_most_active()

        else:
           self.pl = self.compute_profit_loss()
           get_logger().info("profit and loss", pl = self.pl, event_id = self.event_id)

        self.cashout()
        return True

    def cashout(self):
        selection_id = self.list_runner[self.active]["selection_id"]
        market_id = self.list_runner[self.active]["market_id"]
        pc = Execution(self.client, market_id=market_id, selection_id=selection_id)
        unhedged_position = pc.compute_unhedged_position()

        if pc.ask_for_price():
            current_lay = pc.current_lay
            if current_lay is None:
                return None

            get_logger().info("cashout", cashout = unhedged_position / current_lay)
            lay_hedge = unhedged_position / current_lay * 0.5

            if lay_hedge > self.target_profit:
                pc.cashout(0.5)

    def place_passif_bet(self):
        selection_id = self.list_runner[self.active]["selection_id"]
        market_id = self.list_runner[self.active]["market_id"]
        size = self.stake
        if self.current_lay is None:
            price = nearest_price(max(self.current_back * 10, 200))
        else:
            price = price_ticks_away(self.current_lay, -1)
        pricer = Execution(self.client, market_id, selection_id)
        pricer.quote(price, size, Side.BACK)


