import datetime
import queue
from datetime import datetime, timedelta
from time import sleep

from betfair.models import MarketFilter, TimeRange
from structlog import get_logger

from betfair_wrapper.authenticate import authenticate

from betfair_wrapper.authenticate import client_manager
from strategy_handlers.strategyPlayer import StrategyPlayer
class strategy_manager():
    def __init__(self, strategy, event_id = None, number_threads = 1, time_filter = None, inplay_only = False, **params):
        self.client = authenticate()
        self.type_ids = [1]
        self.queue = queue.Queue()
        self.thread_pool = {}
        self.max_threads = number_threads
        self.traded_events = []
        self.client_manager = client_manager(self.client)
        self.strategy = strategy
        self.client_manager.start()
        self.inplay_only = inplay_only
        self.params = params
        self.event_id = event_id

        if time_filter is None:
            self.time_filter_from = -60*1
            self.time_filter_to = 60*1
        else:
            self.time_filter_from = time_filter[0]
            self.time_filter_to = time_filter[1]

    def retrieve_events(self):
        get_logger().info("fetching events")
        if self.event_id is not None:
            events = self.client.list_events(
                MarketFilter(event_ids=[self.event_id])
            )
            return events

        actual_time = datetime.utcnow()
        time_from = (actual_time + timedelta(minutes=self.time_filter_from)).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        time_to = (actual_time + timedelta(minutes=self.time_filter_to)).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        try:
            events = self.client.list_events(
                MarketFilter(event_type_ids=self.type_ids, in_play_only = False,
                             market_start_time = TimeRange(from_ = time_from, to = time_to)),
            )
        except:
            get_logger().info("error while getting events")
            sleep(10)
            return self.retrieve_events()

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
            self.thread_pool[event_id] = StrategyPlayer(self.queue,  self.client, self.strategy, event_id, **self.params)
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


