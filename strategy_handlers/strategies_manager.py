import datetime
import queue
import traceback
from datetime import datetime, timedelta
from time import sleep

from structlog import get_logger

from betfair_wrapper.betfair_wrapper_api import get_api

from betfair_wrapper.authenticate import client_manager
from strategy_handlers.strategyPlayer import StrategyPlayer
class strategy_manager():
    def __init__(self, strategy, event_id = None, number_threads = 1, time_filter = None, inplay_only = False, **params):
        self.type_ids = [1]
        self.queue = queue.Queue()
        self.thread_pool = {}
        self.max_threads = number_threads
        self.traded_events = []
        self.client_manager = client_manager()
        self.strategy = strategy
        self.client_manager.start()
        self.inplay_only = inplay_only
        self.params = params
        self.event_id = event_id

        if time_filter is None:
            self.time_filter_from = -60*1
            self.time_filter_to = 60*2
        else:
            self.time_filter_from = time_filter[0]
            self.time_filter_to = time_filter[1]

    def retrieve_events(self):
        get_logger().info("fetching events")

        actual_time = datetime.utcnow()
        time_from = (actual_time + timedelta(minutes=self.time_filter_from)).strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        time_to = (actual_time + timedelta(minutes=self.time_filter_to)).strftime('%Y-%m-%dT%H:%M:%S.%f%z')

        events = get_api().get_events(self.event_id, [self.type_ids], self.inplay_only, time_from, time_to)

        return events

    def event_generator(self):
        while True:
            events = self.retrieve_events()
            event = None
            while events:
                event = events.pop()

                if event.event.id in self.traded_events:
                    get_logger().info("found already traded", event_name=event.event.name, event_id=event.event.id,
                                      numer_event_traded = len(self.traded_events))
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
        try:
            for event in self.event_generator():

                event_id = event.event.id
                get_logger().info("creating thread for strategy", event_id = event_id, event_name = event.event.name)
                self.thread_pool[event_id] = StrategyPlayer(self.queue, self.strategy, event_id, **self.params)
                self.thread_pool[event_id].start()

                if len(self.thread_pool) >= self.max_threads:
                    get_logger().info("strategy pool is full, block waiting ...")
                    event_id = self.queue.get(True)
                    get_logger().info("strategy finished, removing from the pool", event_id = event_id)
                    self.thread_pool[event_id].join()
                    del self.thread_pool[event_id]
        except Exception as e:
            get_logger().error(traceback.format_exc())
            get_logger().info("closing all threads")

            for thread in self.thread_pool.values():
                thread.stop()
                thread.join()

            get_logger().info("closing strategy manager")

        finally:
            get_logger().info("stopping client manager")
            self.client_manager.stop()
            self.client_manager.join()


