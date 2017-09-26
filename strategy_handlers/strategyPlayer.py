
from strategy_handlers_draw.DrawChaser import Draw_Market
from time import sleep
from structlog import get_logger
import time
import threading
import queue
import traceback
class StrategyPlayer(threading.Thread):
    def __init__(self, queue, client, strategy, event_id, heartbeat = 30, *params):
        threading.Thread.__init__(self)
        self.queue = queue
        self.client = client
        self.event = event_id
        self.heartbeat = heartbeat
        self.strategy = strategy(event_id, client, *params)

        get_logger().info("creating strategy", event_id = event_id, heartbeat = heartbeat, strategy = strategy)

    def run(self):
        while True:
            try:
                still_alive = self.strategy.looper()
            except Exception as e:
                get_logger().error("strategy failed", event_id = self.event)
                get_logger().error(traceback.format_exc())
                still_alive = False
            if not still_alive:
                self.strategy.cancel_all_pending_orders()
                self.queue.put(self.event)
                break
            sleep(self.heartbeat)