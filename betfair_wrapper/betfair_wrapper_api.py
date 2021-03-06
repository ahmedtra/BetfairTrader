import threading
from time import sleep

from requests import ReadTimeout
from schematics.exceptions import ModelValidationError

from betfair_wrapper.authenticate import get_client
from betfair.constants import PriceData, OrderType, PersistenceType, RollupModel, MarketProjection, MarketSort
from betfair.models import PriceProjection, PlaceInstruction, LimitOrder, ReplaceInstruction, \
    CancelInstruction, ExBestOffersOverrides, MarketFilter, TimeRange

from common import singleton
from structlog import get_logger
soccer_type_ids = [1]

DIGIT_ROUND = 2
MINIMUM_SIZE = 4
NUMBER_TRIALS = 5
api = None

def handle_connection(func):
    global connection_lock

    def wrapper(self, *args, **kwargs):

        if hasattr(self, "client"):
            tries = 0
            response = None
            while tries < NUMBER_TRIALS:
                try:
                    return func(self, *args, **kwargs)
                except ModelValidationError as e:
                    get_logger().info(e.messages)
                    break
                except ReadTimeout as e:
                    get_logger().error(e.args)
                    break
                except Exception as e:
                    get_logger().info("connection failed, reconnecting")
                    tries += 1
                    print("trial "+str(tries))
                    sleep(tries * 30)
                    self.client = get_client(True)


            if tries == NUMBER_TRIALS:
                raise ApiFailure("unable to reconnect")
        else:
            response = func(self, *args, **kwargs)
        return response
    return wrapper

class BetfairApiWrapper():
    def __init__(self):
        self.client = get_client()

    @handle_connection
    def place_bet(self, price, size, side, market_id, selection_id, customer_order_ref=None):
        size = round(size, DIGIT_ROUND)
        size_reduction = 0

        if size < MINIMUM_SIZE:
            size_reduction = MINIMUM_SIZE - size

        order = PlaceInstruction()
        order.order_type = OrderType.LIMIT
        order.selection_id = selection_id
        order.side = side
        limit_order = LimitOrder()
        if size_reduction > 0:
            if side == "BACK":
                limit_order.price = 1000
            else:
                limit_order.price = 1.01
        else:
            limit_order.price = price
        limit_order.size = max(size, 4)
        limit_order.persistence_type = PersistenceType.LAPSE
        order.limit_order = limit_order
        if customer_order_ref is not None:
            order.customer_order_ref = customer_order_ref

        instructions = [order]

        response = self.client.place_orders(market_id, instructions, customer_order_ref)

        match = {}
        match["bet_id"] = response.instruction_reports[0].bet_id
        match["price"] = response.instruction_reports[0].average_price_matched
        match["size"] = response.instruction_reports[0].size_matched
        if size_reduction > 0:
            bet_id = match["bet_id"]
            self.cancel_order(market_id, bet_id, size_reduction)
            self.replace_order(market_id, bet_id, price)
        get_logger().debug("place bet", bet_id = match["bet_id"], price = match["price"], size = match["size"])
        return match

    @handle_connection
    def cancel_order(self, market_id, bet_id, size_reduction=None):
        if size_reduction is not None:
            size_reduction = round(size_reduction, 2)
        instruction_cancel = CancelInstruction()
        instruction_cancel.bet_id = bet_id
        instruction_cancel.size_reduction = size_reduction
        get_logger().debug("cancel bet", bet_id= bet_id, size=size_reduction)
        return self.client.cancel_orders(market_id, [instruction_cancel])

    @handle_connection
    def replace_order(self, market_id, bet_id, new_price):
        instruction_update = ReplaceInstruction()
        instruction_update.bet_id = bet_id
        instruction_update.new_price = new_price

        response = self.client.replace_orders(market_id, [instruction_update])

        match = {}

        match["bet_id"] = response.instruction_reports[0].place_instruction_report.bet_id
        match["price"] = response.instruction_reports[0].place_instruction_report.average_price_matched
        match["size"] = response.instruction_reports[0].place_instruction_report.size_matched
        get_logger().debug("replace bet", bet_id=match["bet_id"], price=match["price"], size=match["size"])
        return match

    @handle_connection
    def get_price_market_selection(self,  market_id, selection_id):
        price_projection = PriceProjection()
        price_projection.price_data = [PriceData.EX_BEST_OFFERS]
        price_projection.virtualise = True
        price_projection.rollover_stakes = True
        ex_best_offers_overrides = ExBestOffersOverrides()
        ex_best_offers_overrides.best_prices_depth = 3
        ex_best_offers_overrides.rollup_model = RollupModel.STAKE
        ex_best_offers_overrides.rollup_limit = 1
        price_projection.ex_best_offers_overrides = ex_best_offers_overrides
        books = self.client.list_market_book(market_ids=[market_id], price_projection=price_projection)

        get_logger().debug("asking for runners", market_id = market_id, selection_id = selection_id)
        for runner in books[0].runners:
            if runner.selection_id == selection_id:
                if len(runner.ex.available_to_back) == 0:
                    return None, None, None, None, None
                back = runner.ex.available_to_back[0].price
                lay = None
                if len(runner.ex.available_to_lay) != 0:
                    lay = runner.ex.available_to_lay[0].price
                size = runner.ex.available_to_back[0].size
                orders = runner.orders
                status = runner.status
                if books[0].status == "SUSPENDED":
                    status = "SUSPENDED"
                return back, lay, size, status, orders

        return None, None, None, None, None

    @handle_connection
    def get_placed_orders(self, market_ids):
        response = self.client.list_current_orders(market_ids=market_ids)

        return response.current_orders

    @handle_connection
    def get_markets(self, event_id, text_query=""):

        get_logger().debug("asking for markets", event_id = event_id, text_query = text_query)
        markets = self.client.list_market_catalogue(
            MarketFilter(event_type_ids=soccer_type_ids,
                         text_query=text_query,
                         event_ids=[event_id]),
            max_results=100,
            market_projection=[v.name for v in MarketProjection],
            sort=MarketSort.MAXIMUM_TRADED
        )
        return markets

    def get_runners(self, markets):
        runner_list = {}
        for market in markets:
            market_name = market._data["market_name"]
            total_matched = market._data["total_matched"]
            runners = market._data["runners"]
            for runner in runners:
                selection_id = runner.selection_id
                runner_list[selection_id] = {}
                runner_list[selection_id] = runner._data
                runner_list[selection_id]["market_name"] = market_name
                runner_list[selection_id]["market_id"] = market._data["market_id"]
                runner_list[selection_id]["market_start_time"] = market._data["market_start_time"]
                runner_list[selection_id]["event_name"] = market._data["event"]["name"]
                runner_list[selection_id]["timezone"] = market._data["event"]["timezone"]
                runner_list[selection_id]["event_id"] = market._data["event"]["id"]
                runner_list[selection_id]["total_matched"] = total_matched
        return runner_list

    def get_runner_under(self, markets):
        runner_list = {}
        for market in markets:
            market_name = market._data["market_name"]
            if "Over/Under" in market_name:
                number_goals = int(market_name[market_name.find(".5") - 1])
                runner_list[number_goals] = {}
                runners = market._data["runners"]
                for runner in runners:
                    if "Under" in runner._data["runner_name"]:
                        runner_list[number_goals] = runner._data
                        runner_list[number_goals]["market_id"] = market._data["market_id"]
                        runner_list[number_goals]["market_start_time"] = market._data["market_start_time"]
                        runner_list[number_goals]["event_name"] = market._data["event"]["name"]
                        runner_list[number_goals]["timezone"] = market._data["event"]["timezone"]
                        runner_list[number_goals]["event_id"] = market._data["event"]["id"]
        return runner_list

    @handle_connection
    def get_runner_prices(self, runners):
        marketids = [market["market_id"] for market in runners.values()]
        price_projection = PriceProjection()
        price_projection.price_data = [v.name for v in PriceData]
        price_projection.rollover_stakes = False
        price_projection.virtualise = False
        ex_best_offers_overrides = ExBestOffersOverrides()
        ex_best_offers_overrides.best_prices_depth = 1
        ex_best_offers_overrides.rollup_model = RollupModel.STAKE
        ex_best_offers_overrides.rollup_limit = 0
        price_projection.ex_best_offers_overrides = ex_best_offers_overrides
        books = self.client.list_market_book(market_ids=marketids, price_projection=price_projection)
        get_logger().debug("asking for prices", market_id=marketids)

        selection_ids = {market["selection_id"]: s for s, market in runners.items()}
        for book in books:
            for runner in book.runners:
                if runner.selection_id in selection_ids:
                    if runner.status is not "ACTIVE":
                        continue

                    runners[selection_ids[runner.selection_id]]["stats"] = runner.status
                    runners[selection_ids[runner.selection_id]]["inplay"] = book.inplay

                    if len(runner.ex.available_to_lay) > 0:
                        runners[selection_ids[runner.selection_id]]["lay"] = runner.ex.available_to_lay[0].price
                        runners[selection_ids[runner.selection_id]]["lay_size"] = runner.ex.available_to_lay[0].size
                    else:
                        runners[selection_ids[runner.selection_id]]["lay"] = None
                        runners[selection_ids[runner.selection_id]]["lay_size"] = None
                    if len(runner.ex.available_to_back) > 0:
                        runners[selection_ids[runner.selection_id]]["back"] = runner.ex.available_to_back[0].price
                        runners[selection_ids[runner.selection_id]]["back_size"] = runner.ex.available_to_back[0].size
                    else:
                        runners[selection_ids[runner.selection_id]]["back"] = None
                        runners[selection_ids[runner.selection_id]]["back_size"] = None

        return runners

    @handle_connection
    def keep_alive(self):
        get_logger().debug("keep alive")
        self.client.keep_alive()

    @handle_connection
    def get_events(self, event_id = None, type_ids = None, inplay = False, time_from = None, time_to = None,
                   market_countries = None):
        get_logger().debug("asking for events", event_id = event_id, inplay = inplay,
                           time_from = time_from, time_to = time_to)

        if event_id is not None:
            events = self.client.list_events(
                MarketFilter(event_ids=[event_id])
            )
            return events

        events = self.client.list_events(
            MarketFilter(event_type_ids=type_ids, in_play_only=False,
                         market_start_time=TimeRange(from_=time_from, to=time_to),
                         market_countries=market_countries),
        )
        return events

@singleton
def get_api():
    global api

    if api is None:
        api = BetfairApiWrapper()

    return api

class client_manager(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._stop_event = threading.Event()

    def run(self):
        while True:
            sleep(120)
            get_api().keep_alive()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


class ApiFailure(Exception):
    def __init__(self,message):
        self.message = message
