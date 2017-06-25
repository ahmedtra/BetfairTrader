from data.trades_loader import Loader
from data.sql_wrapper.connection import initialize_secdb

if __name__ == "__main__":
    initialize_secdb()
    data_loader = Loader()
    result = data_loader.get_all_runner_by("event", market_filter="Over", runner_filter="Over")
    for data in result:
        print("here")
    result = data_loader.load_by_market_runner_filter("Over", "Over")
    print(result)
    print("here")