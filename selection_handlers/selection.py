from data_betfair.query import DBQuery


class Selection():
    def __init__(self, market_id, selection_id):
        self.sqldb= DBQuery()
        self.market_id = market_id
        self.selection_id = selection_id

    def set_runner(self, market_id, selection_id):
        self.market_id = market_id
        self.selection_id = selection_id