from betfair_wrapper.authenticate import get_api
from time import sleep
from selection_handlers.selection import Selection
class priceService(Selection):
    def __init__(self, market_id, selection_id, **kwargs):
        super(priceService, self).__init__(market_id, selection_id)

        self.current_orders = None
        self.current_back = None
        self.current_lay = None
        self.current_size = None
        self.status = None

    def ask_for_price(self):
        self.current_back, self.current_lay, self.current_size, self.status, self.current_orders = \
            get_api().get_price_market_selection(self.market_id, self.selection_id)
        while self.status == "SUSPENDED":
            sleep(10)
            self.current_back, self.lay, self.current_size, self.status, self.current_orders = \
                get_api().get_price_market_selection(self.market_id, self.selection_id)

        if self.status == "ACTIVE":
            return True
        else:
            return False
