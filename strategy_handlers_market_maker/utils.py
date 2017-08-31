import threading
from time import sleep

from betfair.constants import PriceData, MarketProjection, OrderType, PersistenceType
from betfair.models import MarketFilter, PriceProjection, PlaceInstruction, LimitOrder, ReplaceInstruction, \
    CancelInstruction

from betfair_wrapper.authenticate import authenticate

soccer_type_ids = [1]
from list_team import team_list

def get_runner(client, market_id):
    markets = client.list_market_catalogue(
        MarketFilter(event_type_ids=soccer_type_ids,
                     event_types=team_list,
                     market_ids=[market_id]),
        max_results=100,
        market_projection=[v.name for v in MarketProjection]
    )
    runner_list = {}
    for market in markets:
        market_name = market._data["market_name"]
        runners = market._data["runners"]
        for runner in runners:
            selection_id = runner._data["selection_id"]
            runner_list[selection_id] = {}
            runner_list[selection_id] = runner._data
            runner_list[selection_id]["market_id"] = market._data["market_id"]
            runner_list[selection_id]["market_start_time"] = market._data["market_start_time"]
            runner_list[selection_id]["event_name"] = market._data["event"]["name"]
            runner_list[selection_id]["timezone"] = market._data["event"]["timezone"]
            runner_list[selection_id]["event_id"] = market._data["event"]["id"]

    return runner_list



def get_runner_prices(client, market_id, runners):
    marketids = [market_id]
    price_projection = PriceProjection()
    price_projection.price_data = [PriceData.EX_BEST_OFFERS]
    books = client.list_market_book(market_ids=marketids, price_projection=price_projection)
    runner_prices = {}
    selection_ids = {s for s in runners.keys()}
    for book in books:
        for runner in book.runners:
            if runner.selection_id in selection_ids:
                if runner.status is not "ACTIVE":
                    continue
                runner_prices[runner.selection_id] = {}
                runner_prices[runner.selection_id]["stats"] = runner.status
                if len(runner.ex.available_to_lay) > 0:
                    runner_prices[runner.selection_id]["lay"] = runner.ex.available_to_lay[0].price
                    runner_prices[runner.selection_id]["lay_size"] = runner.ex.available_to_lay[0].size
                else:
                    runner_prices[runner.selection_id]["lay"] = None
                    runner_prices[runner.selection_id]["lay_size"] = None
                if len(runner.ex.available_to_back) > 0:
                    runner_prices[runner.selection_id]["back"] = runner.ex.available_to_back[0].price
                    runner_prices[runner.selection_id]["back_size"] = runner.ex.available_to_back[0].size
                else:
                    runner_prices[runner.selection_id]["back"] = None
                    runner_prices[runner.selection_id]["back_size"] = None

    return runner_prices

def place_bet(client, price, size, side, market_id, selection_id):
        size_reduction = 0
        if size < 4:
            size_reduction = 4-size

        order = PlaceInstruction()
        order.order_type = OrderType.LIMIT
        order.selection_id = selection_id
        order.side = side
        limit_order = LimitOrder()
        limit_order.price = price
        limit_order.size = max(size, 4)
        limit_order.persistence_type = PersistenceType.LAPSE
        order.limit_order = limit_order

        instructions = [order]

        response = client.place_orders(market_id, instructions)

        match = {}
        match["bet_id"] = response.instruction_reports[0].bet_id
        match["price"] = response.instruction_reports[0].average_price_matched
        match["size"] = response.instruction_reports[0].size_matched
        if size_reduction > 0:
            bet_id = match["bet_id"]
            cancel_order(client, market_id, bet_id, size_reduction)

        return match

def replace_order(client, market_id, bet_id, new_price):
    instruction_update = ReplaceInstruction()
    instruction_update.bet_id = bet_id
    instruction_update.new_price = new_price

    response = client.replace_orders(market_id, [instruction_update])

    match = {}

    match["bet_id"] = response.instruction_reports[0].place_instruction_report.bet_id
    match["price"] = response.instruction_reports[0].place_instruction_report.average_price_matched
    match["size"] = response.instruction_reports[0].place_instruction_report.size_matched

    return match

def cancel_order(client, market_id, bet_id, size_reduction = None):
    instruction_cancel = CancelInstruction()
    instruction_cancel.bet_id = bet_id
    instruction_cancel.size_reduction = size_reduction

    response = client.cancel_orders(market_id, [instruction_cancel])


def get_price_market_selection(client, market_id, selection_id):
    price_projection = PriceProjection()
    price_projection.price_data = [PriceData.EX_BEST_OFFERS]
    books = client.list_market_book(market_ids=[market_id], price_projection=price_projection)

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
            return back,lay, size, status, orders
    return None, None, None, None, None

def get_placed_orders(client, market_ids):
    response = client.list_current_orders(market_ids= market_ids)
    return response.current_orders

def initialize():
    client = authenticate()
    return client


def get_profit_and_loss(client, market_ids):
    response = client.list_market_profit_and_loss(market_ids)
    return response

class client_manager(threading.Thread):
    def __init__(self, client):
        threading.Thread.__init__(self)
        self.client = client
        self._stop_event = threading.Event()

    def run(self):
        while True:
            sleep(600)
            self.client.keep_alive()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()