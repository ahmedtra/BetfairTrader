from betfair.constants import Side
from betfair.price import price_ticks_away, nearest_price
from structlog import get_logger

from strategy_handlers_draw.priceChaser import PriceChaser
from strategy_handlers_draw.utils import get_placed_orders, get_profit_and_loss, \
    get_runner_prices, get_runners, get_odds_markets
from strategy_handlers_draw.pricer import Pricer

MAX_STAKE = 4
MIN_STAKE = 4

class Draw_Market():
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
        self.the_draw = None
        self.traded = False
        self.list_runner = self.create_runner_info()
        self.prices = {}
        self.win = {}
        self.lost = {}
        self.traded_account = []
        self.inplay = False

    def create_runner_info(self):
        get_logger().info("checking for runner under market", event_id = self.event_id)
        markets = get_odds_markets(self.client, self.event_id)
        get_logger().info("got markets", number_markets = len(markets), event_id = self.event_id)
        return get_runners(markets)

    def get_draw_bet(self):
        get_logger().info("getting draw runner")
        self.the_draw = None
        for runner in self.list_runner.values():
            if runner["selection_id"] not in self.win.keys():
                self.win[runner["selection_id"]] = 0
                self.lost[runner["selection_id"]] = 0
            if runner["runner_name"] == "The Draw":
                self.the_draw = runner["selection_id"]
        
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
        selection_id = self.list_runner[self.the_draw]["selection_id"]
        market_id = self.list_runner[self.the_draw]["market_id"]
        pc = PriceChaser(self.client, market_id=market_id, selection_id = selection_id)

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

                if self.prices[self.the_draw]["back"] < 1.2:
                    get_logger().info("very low price, taking loss, quitting",
                                      spread=self.prices[self.the_draw]["back"], event_id=self.event_id)
                    return False


                self.place_bet_on_the_draw()

        else:
           self.pl = self.compute_profit_loss()
           get_logger().info("profit and loss", pl = self.pl, event_id = self.event_id)

        unhedged_position = self.compute_unhedged_position()
        self.cashout(unhedged_position)
        return True

    def compute_unhedged_position(self):
        selection_id = self.list_runner[self.the_draw]["selection_id"]
        market_id = self.list_runner[self.the_draw]["market_id"]
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
            win_outcome = self.win[self.the_draw]

        lost_outcome = - back_position + lay_position
        if lost_outcome == 0:
            lost_outcome = self.lost[self.the_draw]

        get_logger().debug("back position", market_id= market_id,
                           back=back_price, stake=back_position)

        get_logger().debug("lay position", market_id= market_id,
                           lay=back_price, stake=back_position)

        get_logger().info("win_outcome", market_id= market_id,
                           hedge=win_outcome)
        get_logger().info("loss_outcome", market_id= market_id,
                           hedge=lost_outcome)

        self.win[self.the_draw] = win_outcome
        self.lost[self.the_draw] = lost_outcome
        return win_outcome

    def cashout(self, unhedged_position):
        selection_id = self.list_runner[self.the_draw]["selection_id"]
        market_id = self.list_runner[self.the_draw]["market_id"]
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

    def place_passif_bet(self):
        selection_id = self.list_runner[self.the_draw]["selection_id"]
        market_id = self.list_runner[self.the_draw]["market_id"]
        size = self.stake
        if self.current_lay is None:
            price = nearest_price(max(self.current_back * 10, 200))
        else:
            price = price_ticks_away(self.current_lay, -1)
        pricer = Pricer(self.client, market_id, selection_id)
        pricer.Price(price, size, Side.BACK)


