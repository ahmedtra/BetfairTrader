from betfair.constants import Side
from structlog import get_logger

from strategy_handlers_under_goals.priceChaser import PriceChaser
from strategy_handlers_under_goals.utils import get_placed_orders, get_profit_and_loss, get_under_over_markets, \
    get_runner_under, \
    get_runner_prices

MAX_STAKE = 4
MIN_STAKE = 4

class Runners_Under_Market():
    def __init__(self, event_id, client):
        get_logger().info("creating Runner_under_market", event_id = event_id)
        self.client = client
        self.target_profit = 5000
        self.current_back = 1
        self.current_lay = 1000
        self.event_id = event_id
        self.stake = 0
        self.pl = 0
        self.already_traded = 0
        self.active = 10
        self.traded = False
        self.list_runner = self.create_runner_info()
        self.prices = {}
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
            price_chaser = PriceChaser(self.client, market_id, selection_id)
            matches = price_chaser.chasePrice(price, size, Side.BACK)
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
           self.pl = self.compute_profit_loss()
           get_logger().info("profit and loss", pl = self.pl, event_id = self.event_id)

        unhedged_position = self.compute_unhedged_position()
        self.cashout(unhedged_position)
        return True

    def compute_unhedged_position(self):
        selection_id = self.list_runner[self.active]["selection_id"]
        market_id = self.list_runner[self.active]["market_id"]
        pc = PriceChaser(self.client, market_id=market_id, selection_id=selection_id)
        matched_orders = []
        matches_back = pc.get_betfair_matches(Side.BACK)
        matched_orders = matched_orders + pc.bet_fair_matches
        matches_lay = pc.get_betfair_matches(Side.LAY)
        matched_orders = matched_orders + pc.bet_fair_matches
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
            win_outcome = self.win[self.active]

        lost_outcome = - back_position + lay_position
        if lost_outcome == 0:
            lost_outcome = self.lost[self.active]

        get_logger().debug("back position", market_id= market_id,
                           back=back_price, stake=back_position)

        get_logger().debug("lay position", market_id= market_id,
                           lay=back_price, stake=back_position)

        get_logger().info("win_outcome", market_id= market_id,
                           hedge=win_outcome)
        get_logger().info("loss_outcome", market_id= market_id,
                           hedge=lost_outcome)

        self.win[self.active] = win_outcome
        self.lost[self.active] = lost_outcome
        return win_outcome

    def cashout(self, unhedged_position):
        selection_id = self.list_runner[self.active]["selection_id"]
        market_id = self.list_runner[self.active]["market_id"]
        pc = PriceChaser(self.client, market_id=market_id, selection_id=selection_id)

        if pc.ask_for_price():
            current_lay = pc.current_lay
            if current_lay is None:
                return None

            get_logger().info("cashout", cashout = unhedged_position / current_lay)
            lay_hedge = unhedged_position / current_lay * 0.5

            if lay_hedge > self.target_profit:
                lay_hedge = round(lay_hedge, 2)
                get_logger().info("lquidating half position")
                pc.chasePrice(current_lay, lay_hedge, Side.LAY, True)

