import pandas as pd

from data.cassandra_wrapper.access import CassTradesRepository
from data.sql_wrapper.query import RunnerMapQuery

class Loader():
    def __init__(self):
        self.cass_repository = CassTradesRepository()
        self.query_secdb = RunnerMapQuery()

    def load_df_data(self, market_id, selection_id):
        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)
        result = self.load_data(market_id, selection_id, row_factory=pandas_factory)
        return result

    def load_data(self, market_id, selection_id, row_factory = None):
        result = self.cass_repository.load_data_async(market_id, selection_id, row_factory = row_factory)
        return result.result()._current_rows

    def create_filter(self, s):
        if s is None:
            return "%%"
        else:
            return "%" + s + "%"

    def load_by_market_runner_filter(self, market_filter = None, runner_filter = None,
                                     competition_filter = None, event_filter = None ):

        results_query = self.query_secdb.get_runner_maps(competition_filter, event_filter, market_filter, runner_filter)

        for runner_map, runner, market, event, competition in results_query:
            df = self.load_df_data(runner_map.market_id, runner_map.selection_id)
            df["runner"] = runner.runner_name
            df["market"] = market.market_name

            yield df

    def get_all_runner_by(self, by = "runner", market_filter = None, event_filter = None
                          , runner_filter = None, competition_filter = None):

        market_filter = self.create_filter(market_filter)
        runner_filter = self.create_filter(runner_filter)
        competition_filter = self.create_filter(competition_filter)
        event_filter = self.create_filter(event_filter)

        if by == "runner":
            return self.load_by_market_runner_filter(market_filter, runner_filter,
                                                     competition_filter, event_filter)
        elif by == "market":
            markets = self.query_secdb.get_markets(market_filter)
            for market in markets:
                all_data = list(self.load_by_market_runner_filter(market.market_name, runner_filter,
                                                     competition_filter, event_filter))
                yield all_data

        elif by == "event":
            events = self.query_secdb.get_events(event_filter)
            for event in events:
                all_data = list(self.load_by_market_runner_filter(market_filter, runner_filter,
                                                     competition_filter, event.name))
                yield all_data
