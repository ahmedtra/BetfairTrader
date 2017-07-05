from data.trades_loader import Loader
from data.sql_wrapper.connection import initialize_secdb
import pandas as pd

if __name__ == "__main__":
    initialize_secdb()
    data_loader = Loader()
    result = data_loader.get_all_runner_by("event", market_filter="Over/Under%", runner_filter="Over%")
    for data in result:
        if len(data) == 0:
            continue
        markets_for_one = pd.concat(data)

    result = data_loader.load_by_market_runner_filter("Over", "Over")
    print(result)
    print("here")