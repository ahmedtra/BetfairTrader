from strategy_handlers_market_maker.marketMaker import MarketMaker
from time import sleep
from structlog import get_logger
import time
import threading
import queue
import traceback
class MarketMakerStrategyPlayer(threading.Thread):
    def __init__(self, queue, client, event_id, heartbeat = 60):
        threading.Thread.__init__(self)
        self.queue = queue
        self.client = client
        self.event = event_id
        self.heartbeat = heartbeat
        self.strategy = MarketMaker(event_id, client)

        get_logger().info("creating under goals strategy", event_id = event_id, heartbeat = heartbeat)

    def run(self):
        while True:
            try:
                still_alive = self.strategy.looper()
            except Exception as e:
                get_logger().error("strategy failed", event_id = self.event)
                get_logger().error(traceback.format_exc())
                still_alive = False
            if not still_alive:
                self.queue.put(self.event)
                break
            sleep(self.heartbeat)