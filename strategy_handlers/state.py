import datetime
from abc import abstractmethod

from data_betfair.query import DBQuery


class State():
    def __init__(self, strategy_id):
        self.strategy_id = strategy_id
        self.sqldb = DBQuery()
        self.saved_states = {}

    def update_state(self, key, value):
        self.saved_states[key] = value

    def save_state(self):
        time = datetime.datetime.now()
        for key, value in self.saved_states.items():
            self.sqldb.add_state(self.strategy_id, time, key, str(value))
