import queue

from betfair.models import MarketFilter
from structlog import get_logger

from authenticate import authenticate
from common import initialize_logging
from list_team import team_list
from strategy_handlers_under_goals.strategyPlayer import UnderGoalsStrategyPlayer
from strategy_handlers_under_goals.utils import client_manager
from time import sleep
initialize_logging("UnderOverStrategy")

class strategy_manager():
    def __init__(self, event_id = None):
        self.client = authenticate()
        self.type_ids = [1]
        self.queue = queue.Queue()
        self.thread_pool = {}
        self.max_threads = 6
        self.traded_events = []
        self.client_manager = client_manager(self.client)
        self.client_manager.start()

    def retrieve_events(self):
        get_logger().info("fetching events")
        events = self.client.list_events(
            MarketFilter(event_type_ids=self.type_ids, event_types=team_list, in_play_only = True)
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
        self.events = self.retrieve_events()

        for event in self.event_generator():

            event_id = event.event.id
            get_logger().info("creating thread for strategy", event_id = event_id, event_name = event.event.name)
            self.thread_pool[event_id] = UnderGoalsStrategyPlayer(self.queue, self.client, event_id)
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