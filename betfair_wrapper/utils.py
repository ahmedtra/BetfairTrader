import threading
from time import sleep

from betfair.constants import PriceData, MarketProjection
from betfair.models import MarketFilter, PriceProjection

from betfair_wrapper.authenticate import authenticate

soccer_type_ids = [1]


def get_markets(client, event_id, text_query = ""):

    markets = client.list_market_catalogue(
        MarketFilter(event_type_ids=soccer_type_ids,
                     text_query = text_query,
                     event_ids = [event_id]),
        max_results=100,
        market_projection=[v.name for v in MarketProjection]
    )
    return markets

def get_runners(markets):
    runner_list = {}
    for market in markets:
        market_name = market._data["market_name"]
        runners = market._data["runners"]
        for runner in runners:
            selection_id = runner.selection_id
            runner_list[selection_id] = {}
            runner_list[selection_id] = runner._data
            runner_list[selection_id]["market_id"] = market._data["market_id"]
            runner_list[selection_id]["market_start_time"] = market._data["market_start_time"]
            runner_list[selection_id]["event_name"] = market._data["event"]["name"]
            runner_list[selection_id]["timezone"] = market._data["event"]["timezone"]
            runner_list[selection_id]["event_id"] = market._data["event"]["id"]

    return runner_list

def get_runner_under(markets):
    runner_list = {}
    for market in markets:
        market_name = market._data["market_name"]
        if "Over/Under" in market_name:
            number_goals = int(market_name[market_name.find(".5")-1])
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

def get_runner_prices(client, markets):
    marketids = [market["market_id"] for market in markets.values()]
    price_projection = PriceProjection()
    price_projection.price_data = [PriceData.EX_BEST_OFFERS]
    books = client.list_market_book(market_ids=marketids, price_projection=price_projection)
    runner_prices = {}
    selection_ids = {market["selection_id"]:s for s, market in markets.items()}
    for book in books:
        for runner in book.runners:
            if runner.selection_id in selection_ids:
                if runner.status is not "ACTIVE":
                    continue

                runner_prices[selection_ids[runner.selection_id]] = {}
                runner_prices[selection_ids[runner.selection_id]]["stats"] = runner.status
                runner_prices[selection_ids[runner.selection_id]]["inplay"] = book.inplay

                if len(runner.ex.available_to_lay) > 0:
                    runner_prices[selection_ids[runner.selection_id]]["lay"] = runner.ex.available_to_lay[0].price
                    runner_prices[selection_ids[runner.selection_id]]["lay_size"] = runner.ex.available_to_lay[0].size
                else:
                    runner_prices[selection_ids[runner.selection_id]]["lay"] = None
                    runner_prices[selection_ids[runner.selection_id]]["lay_size"] = None
                if len(runner.ex.available_to_back) > 0:
                    runner_prices[selection_ids[runner.selection_id]]["back"] = runner.ex.available_to_back[0].price
                    runner_prices[selection_ids[runner.selection_id]]["back_size"] = runner.ex.available_to_back[0].size
                else:
                    runner_prices[selection_ids[runner.selection_id]]["back"] = None
                    runner_prices[selection_ids[runner.selection_id]]["back_size"] = None

    return runner_prices



def initialize():
    client = authenticate()
    return client

