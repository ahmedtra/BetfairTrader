from common import initialize_logging
from data.trades_export import Recorder
from data.sql_wrapper.connection import initialize_secdb

if __name__ == "__main__":
    initialize_secdb()
    initialize_logging("decompress_betfair_data")

    json_trades_recorder = Recorder()
    json_trades_recorder.read_json_files()



