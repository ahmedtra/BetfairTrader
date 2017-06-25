from data.cassandra_wrapper.access import CassQuoteRepository
import pandas as pd

class DataLoader():
    def __init__(self, market_id, selection_id):
        self.market_id = market_id
        self.selection_id = selection_id
        self.cassandra_repository = CassQuoteRepository()

    def load_df_data(self):
        def pandas_factory(colnames, rows):
            return pd.DataFrame(rows, columns=colnames)
        result = self.load_data(row_factory=pandas_factory)
        return result

    def load_data(self, row_factory = None):
        result = self.cassandra_repository.load_data_async(self.market_id, self.selection_id, row_factory = row_factory)
        return result.result()._current_rows
