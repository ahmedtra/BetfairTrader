
from time import sleep
from structlog import get_logger
import threading

import traceback
class StrategyPlayer(threading.Thread):
    def __init__(self, queue, strategy, event_id, event_name = None, heartbeat = 30, **params):
        threading.Thread.__init__(self)
        self.queue = queue
        self.event = event_id
        self.event_name = event_name
        self.heartbeat = heartbeat
        self.strategy = strategy(event_id, event_name, **params)
        self.strategy.add_strategy(status = "active")
        self._stop_event = threading.Event()

        get_logger().info("creating strategy", event_id = event_id, heartbeat = heartbeat, strategy = strategy)

    def run(self):
        failed = False
        while True:
            try:
                still_alive = self.strategy.looper()
                self.strategy.state.save_state()
            except Exception as e:
                get_logger().error("strategy failed", event_id = self.event)
                get_logger().error(traceback.format_exc())
                still_alive = False
                self.strategy.add_strategy("failed")
                failed = True
            if not still_alive:
                self.strategy.cancel_all_pending_orders()
                self.queue.put(self.event)
                if not failed:
                    self.strategy.add_strategy("finished")
                break
            sleep(self.heartbeat)

    def stop(self):
        self.strategy.cancel_all_pending_orders()
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()