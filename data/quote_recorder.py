from betfair.constants import PriceData, MarketProjection
from betfair.models import PriceProjection, MarketFilter
from datetime import datetime
from time import sleep
from cassandra.query import UNSET_VALUE
from structlog import get_logger

from data.cassandra_wrapper.access import CassQuoteRepository
from data.cassandra_wrapper.model import Quote
from list_team import team_list


class Recorder():
    def __init__(self, client, event_type):

        self.event_type = event_type

        self.client = client
        self.market_ids = []
        self.cass_repository = CassQuoteRepository()

    def get_runner_prices_and_save(self):
            price_projection = PriceProjection()
            price_projection.price_data = [PriceData.EX_BEST_OFFERS, PriceData.EX_TRADED]
            iter_books = self.client.iter_list_market_book(market_ids= self.market_ids, price_projection=price_projection, chunk_size=10)
            timestamp = datetime.utcnow()
            all_runners = []
            for book in iter_books:
                for runner in book.runners:
                    runner_dict = {}

                    runner_dict["market_id"] = book.market_id
                    runner_dict["selection_id"] = runner.selection_id

                    runner_dict["timestamp"] = timestamp
                    runner_dict["status"] = runner.status
                    runner_dict["total_matched"] = runner.total_matched
                    runner_dict["last_price_traded"] = runner.last_price_traded
                    runner_dict["inplay"] = book.inplay

                    len_book_lay = len(runner.ex.available_to_lay)
                    for i in range(len_book_lay):
                        runner_dict["lay_"+str(i+1)] = runner.ex.available_to_lay[i].price
                        runner_dict["lay_size_"+str(i+1)] = runner.ex.available_to_lay[i].size

                    len_book_back = len(runner.ex.available_to_back)
                    for i in range(len_book_back):
                        runner_dict["back_" + str(i+1)] = runner.ex.available_to_back[i].price
                        runner_dict["back_size_"+str(i+1)] = runner.ex.available_to_back[i].size

                    for i in range(len_book_lay, 3):
                        runner_dict["lay_" + str(i + 1)] = None
                        runner_dict["lay_size_" + str(i + 1)] = None

                    len_book_back = len(runner.ex.available_to_back)
                    for i in range(len_book_back):
                        runner_dict["back_" + str(i + 1)] = runner.ex.available_to_back[i].price
                        runner_dict["back_size_" + str(i + 1)] = runner.ex.available_to_back[i].size

                    for i in range(len_book_back, 3):
                        runner_dict["back_" + str(i + 1)] = None
                        runner_dict["back_size_" + str(i + 1)] = None

                    all_runners.append(runner_dict)

            self.record(all_runners)

    def record(self, runner_dict):
        self.cass_repository.save_async(runner_dict)

    def update_market_list(self):
        markets = self.client.list_market_catalogue(
            MarketFilter(
                         event_types_ids=self.event_type,
                         in_play_only = True,
                         ),
            max_results=1000,
        )

        self.market_ids = [market["market_id"] for market in markets]

    def looper(self):
        counter = 0
        while True:
            if counter <= 0:
                counter = 120
                self.update_market_list()
            self.get_runner_prices_and_save()
            counter -=1
            sleep(30)



