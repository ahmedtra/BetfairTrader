from betfair.constants import PriceData, MarketProjection
from betfair.models import PriceProjection, MarketFilter
from datetime import datetime
from time import sleep
from os import listdir
from os.path import isfile, join
import pandas as pd
import json
from common import safe_move, get_json_files_dirs

from data.cassandra_wrapper.access import CassTradesRepository
from data.sql_wrapper.query import DBQuery


class Recorder():
    def __init__(self):
        dir_origin, dir_completed = get_json_files_dirs()
        self.path = dir_origin
        self.path_completed = dir_completed
        self.cass_repository = CassTradesRepository()
        self.query_secdb = DBQuery()

    def read_json_files(self):

        files = [f for f in listdir(self.path) if isfile(join(self.path, f))]

        for filename in files:
            filepath = join(self.path, filename)

            market_def = {}

            runners = {}
            file_data = []
            for line in open(filepath):
                json_file = json.loads(line)

                if "marketDefinition" in list(json_file["mc"][0].keys()):
                    market_def = json_file["mc"][0]["marketDefinition"]
                    market_def["market_id"] = json_file["mc"][0]["id"]
                    if "runners" in market_def.keys():
                        for runner in market_def["runners"]:
                            runners[runner["id"]] = runner

                    self.record_market_defintion(market_def)

                if "rc" not in list(json_file["mc"][0].keys()):
                    continue

                for ltp in json_file["mc"][0]["rc"]:
                    data = {}
                    data["ltp"] = ltp["ltp"]
                    data["market_id"] = json_file["mc"][0]["id"]
                    data["selection_id"] = ltp["id"]
                    if ltp["id"] in list(runners.keys()):
                        data["status"] = runners[ltp["id"]]["status"]
                        data["inplay"] = market_def["inPlay"]
                    data["timestamp"] = datetime.fromtimestamp(json_file["pt"] / 1000)

                    self.record_trade(data)

            self.query_secdb.commit_changes()

            file_completed = join(self.path_completed, filename)

            safe_move(filepath, file_completed)

    def record_trade(self, data):
        self.cass_repository.save_async(data)

    def record_market_defintion(self, market_def):
        market_start_time = datetime.strptime(market_def["marketTime"], "%Y-%m-%dT%H:%M:%S.%fZ")
        if "countryCode" not in market_def.keys():
            market_def["countryCode"] = None
        if "timezone" not in market_def.keys():
            market_def["timezone"] = None
        if "bettingType" not in market_def.keys():
            market_def["bettingType"] = None

        self.query_secdb.add_event(market_def["eventId"], market_def["eventName"],
                                   market_def["countryCode"], market_def["timezone"],
                                   None, market_start_time, None, 1)
        self.query_secdb.add_market(market_def["market_id"], market_def["name"],
                                    market_start_time, market_def["bettingType"],
                                    market_def["eventId"])

        if "runners" in market_def.keys():
            for runner in market_def["runners"]:
                self.query_secdb.add_runner(runner["id"], runner["name"], None, runner["sortPriority"], None)
                self.query_secdb.add_runner_map(market_def["market_id"], runner["id"])





