class Selection():
    def __init__(self, client, market_id, selection_id):
        self.client = client
        self.market_id = market_id
        self.selection_id = selection_id

    def set_runner(self, market_id, selection_id):
        self.market_id = market_id
        self.selection_id = selection_id