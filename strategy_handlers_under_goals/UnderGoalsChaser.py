from structlog import get_logger

from strategy_handlers_under_goals.priceChaser import PriceChaser
from strategy_handlers_under_goals.utils import get_placed_orders, get_profit_and_loss, get_under_over_markets, \
    get_runner_under, \
    get_runner_prices

MAX_STAKE = 20
MIN_STAKE = 4

class Runners_Under_Market():
    def __init__(self, event_id, client):
        get_logger().info("creating Runner_under_market", event_id = event_id)
        self.client = client
        self.target_profit = 4
        self.current_back = 1
        self.current_lay = 1000
        self.event_id = event_id
        self.stake = 0
        self.already_traded = 0
        self.active = 10
        self.traded = False
        self.list_runner = self.create_runner_info()
        self.prices = {}
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
        get_logger().info("computed active bet", active = most_active, price = self.current_back, event_id = self.event_id)
        print("price "+str(self.current_back))
        return most_active_updated

    def create_runner_info(self):
        get_logger().info("checking for runner under market", event_id = self.event_id)
        markets = get_under_over_markets(self.client, self.event_id)
        get_logger().info("got markets", number_markets = len(markets), event_id = self.event_id)
        return get_runner_under(markets)

    def update_runner_current_price(self):
        get_logger().info("retriving prices", event_id = self.event_id)
        self.prices = get_runner_prices(self.client, self.list_runner)
        for p in self.prices.values():
            if p["lay"] is not None and p["back"] is not None:
                p["spread"] = 2 * (p["lay"] - p["back"]) / (p["lay"] + p["back"]) * 100
            else:
                p["spread"] = None
        get_logger().info("updated the prices", event_id = self.event_id)
        return self.prices

    def compute_stake(self):
        get_logger().debug("computing stake", event_id = self.event_id)
        already_traded = 0
        for traded in self.traded_account:
            already_traded += traded["size"]
        self.already_traded = already_traded

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
            price_chaser = PriceChaser(self.client, market_id, selection_id)
            matches = price_chaser.chasePrice(price, size)
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
        pc = PriceChaser(self.client, market_id=market_id, selection_id = selection_id)
        matches = pc.get_betfair_matches()

        profit = 0

        if pc.ask_for_price():
            current_lay = pc.current_lay
            if current_lay is None:
                return None
            for match in pc.bet_fair_matches:
                back_match = match["price"]
                size_match = match["size"]
                pl = size_match * (back_match - current_lay) / current_lay
                profit += pl

        return profit

    def get_traded_amount(self):
        already_traded = 0
        for trades in self.traded_account:
            already_traded += trades["size"]

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
            get_logger().info("no runner, skipping iteration", event_id = self.event_id)
            return False

        get_logger().info("starting iteration", traded = self.traded, event_id = self.event_id)

        self.find_most_active()

        if self.active == 10:
            get_logger().info("no active bet, quitting strategy", event_id = self.event_id)
            return False

        if not self.traded:
            self.compute_stake()

            if self.prices[self.active]["spread"]>20 and self.already_traded == 0:
                get_logger().info("very wide spread, strategy not invested, quitting",
                                  spread = self.prices[self.active]["spread"], event_id = self.event_id)
                return False

            if self.prices[self.active]["back"] < 2 and self.already_traded == 0:
                get_logger().info("very low price, strategy not invested, quitting",
                                  spread=self.prices[self.active]["back"], event_id = self.event_id)
                return False

            if self.prices[self.active]["back"] < 1.2:
                get_logger().info("very low price, taking loss, quitting",
                                  spread=self.prices[self.active]["back"], event_id=self.event_id)
                return False

            self.place_bet_on_most_active()

        else:
            pl = self.compute_profit_loss()
            get_logger().info("profit and loss", pl = pl, event_id = self.event_id)

        return True

