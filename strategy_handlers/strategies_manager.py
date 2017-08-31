import datetime
import queue
from datetime import datetime, timedelta
from time import sleep

from betfair.models import MarketFilter, TimeRange
from structlog import get_logger

from betfair_wrapper.authenticate import authenticate
from common import initialize_logging
from strategy_handlers_draw.utils import client_manager

initialize_logging("DrawStrategy")

class strategy_manager():
    def __init__(self, strategy, event_id = None, time_filter = None, inplay_only = False):
        self.client = authenticate()
        self.type_ids = [1]
        self.queue = queue.Queue()
        self.thread_pool = {}
        self.max_threads = 1
        self.traded_events = []
        self.client_manager = client_manager(self.client)
        self.strategy = strategy
        self.client_manager.start()
        self.inplay_only = inplay_only
        if time_filter is None:
            self.time_filter_from = 30 * 5
            self.time_filter_to = 30
        else:
            self.time_filter_from = time_filter[0]
            self.time_filter_to = time_filter[1]

    def retrieve_events(self):
        get_logger().info("fetching events")
        actual_time = datetime.now()
        time_from = (actual_time - timedelta(minutes=self.time_filter_from)).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        time_to = (actual_time + timedelta(minutes=self.time_filter_to)).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        events = self.client.list_events(
            MarketFilter(event_type_ids=self.type_ids, in_play_only = False,
                         market_start_time = TimeRange(from_ = time_from, to = time_to)),
        )
        get_logger().info("fetching all events", number_events = len(events))
        return events

    def event_generator(self):
        while True:
            events = self.retrieve_events()
            event = None
            while events:
                event = events.pop()

                if event.event.id in self.traded_events:
                    get_logger().info("found already traded", event_name=event.event.name, event_id=event.event.id)
                    event = None
                    continue

                get_logger().info("found event to trade", event_name = event.event.name, event_id = event.event.id)
                self.traded_events.append(event.event.id)
                break

            if event is None:
                get_logger().info("no event waiting")
                sleep(120)
            else:
                yield event

    def manage_strategies(self):

        for event in self.event_generator():

            event_id = event.event.id
            get_logger().info("creating thread for strategy", event_id = event_id, event_name = event.event.name)
            self.thread_pool[event_id] = self.strategy(self.queue, self.client, event_id)
            self.thread_pool[event_id].start()

            if len(self.thread_pool) >= self.max_threads:
                get_logger().info("strategy pool is full, block waiting ...")
                event_id = self.queue.get(True)
                get_logger().info("strategy finished, removing from the pool", event_id = event_id)
                self.thread_pool[event_id].join()
                del self.thread_pool[event_id]

        get_logger().info("stopping client manager")
        self.client_manager.stop()
        self.client_manager.join()



if __name__ == "__main__":
    sm = strategy_manager()
    sm.manage_strategies()