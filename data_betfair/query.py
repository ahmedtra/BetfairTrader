from datetime import datetime

from structlog import get_logger

from data_betfair.model import Competitions, Events, Runners, Markets, RunnersMap, EventTypes, Strategies, Orders
from data_betfair.connection import get_session

class DBQuery():
    def __init__(self):
        pass

    def commit_changes(self):
        get_session().commit()

    def _add_flush_to_secdb(self, obj):
        """Add object and flush to securities database"""
        get_session().add(obj)
        self._flush_changes()

    def _flush_changes(self):
        get_session().flush()

    def add_event_type(self, event_type_id, event_type_name, market_count):
        res = get_session().query(EventTypes) \
        .filter(EventTypes.event_type_id == event_type_id)\
            .all()

        if len(res) == 0:
            event_type = EventTypes(event_type_id = event_type_id, name = self.safe_str(event_type_name),
                                       market_count = market_count)
            self._add_flush_to_secdb(event_type)
        else:
            event_type = res[0]
            event_type.name = self.safe_str(event_type_name)
            event_type.market_count = market_count
            self._flush_changes()

    def get_event_types(self):
        res = get_session().query(EventTypes).all()
        return res

    def add_competition(self, competition_id, name, region, market_count, event_type_id):
        res = get_session().query(Competitions) \
        .filter(Competitions.competition_id == competition_id)\
            .all()

        if len(res) == 0:
            competition = Competitions(competition_id = competition_id, name = self.safe_str(name)
                                       , competition_region = self.safe_str(region), market_count = market_count,
                                       event_type_id=event_type_id)
            self._add_flush_to_secdb(competition)
        else:
            competition = res[0]
            competition.name = self.safe_str(name)
            competition.region = self.safe_str(region)
            competition.market_count = market_count
            competition.event_type_id = event_type_id
            self._flush_changes()

    def get_competitions(self):
        res = get_session().query(Competitions).all()
        return res

    def add_event(self, event_id, name, country_code, timezone, venue,
                  open_date, market_count,  competition_id):
        res = get_session().query(Events) \
            .filter(Events.event_id == event_id) \
            .all()

        if len(res) == 0:
            event = Events(event_id=event_id, name=self.safe_str(name), country_code = country_code,
                           timezone = timezone, venue = venue, open_date = open_date,
                           market_count = market_count, competition_id = competition_id)
            self._add_flush_to_secdb(event)
        else:
            event = res[0]
            event.name = self.safe_str(name)
            event.country_code = country_code
            event.timezone = timezone
            event.venue = venue
            event.open_date = open_date
            event.market_count = market_count
            event.competition_id = competition_id
            self._flush_changes()

    def get_events(self):
        res = get_session().query(Events) \
            .all()
        return res

    def add_market(self, market_id, market_name, market_start_time, description, event_id):
        res = get_session().query(Markets) \
            .filter(Markets.market_id == market_id) \
            .all()

        if len(res) == 0:
            market = Markets(market_id = market_id, market_name = market_name, market_start_time = market_start_time,
                            description = description,  event_id = event_id)
            self._add_flush_to_secdb(market)
        else:
            market = res[0]
            market.market_name = self.safe_str(market_name)
            market.market_start_time = market_start_time
            market.description = self.safe_str(description)
            market.event_id = event_id
            self._flush_changes()

        return market


    def add_runner(self, selection_id, runner_name, handicap, sort_priority, meta_data_id):
        res = get_session().query(Runners) \
            .filter(Runners.selection_id == selection_id) \
            .all()
        get_logger().info("adding runner", selection_id=selection_id, name=self.safe_str(runner_name),
                          handicap=handicap, sort_priority=sort_priority)

        if len(res) == 0:
            runner = Runners(selection_id = selection_id, runner_name = self.safe_str(runner_name),
                            handicap = handicap, sort_priority = sort_priority, meta_data_id = meta_data_id)
            self._add_flush_to_secdb(runner)
        else:
            runner = res[0]
            runner.selection_id = selection_id
            runner.runner_name = self.safe_str(runner_name)
            runner.handicap = handicap
            runner.sort_priority = sort_priority
            runner.meta_data_id = meta_data_id
            self._flush_changes()
        return runner

    def add_runner_map(self, market_id, selection_id):
        res = get_session().query(RunnersMap)\
            .filter(RunnersMap.market_id == market_id, RunnersMap.selection_id == selection_id)\
            .all()

        if len(res) == 0:
            runner_map = RunnersMap(market_id = market_id, selection_id = selection_id)
            self._add_flush_to_secdb(runner_map)
        else:
            runner_map = res[0]
            runner_map.market_id = market_id
            runner_map.selection_id = selection_id
            self._flush_changes()
        return runner_map

    def add_strategy(self, name , event, event_name):
        res = get_session().query(Strategies) \
            .filter(Strategies.name == name, Strategies.event == event) \
            .all()

        if len(res) == 0:
            strategy = Strategies(name = name, event = event, event_name = event_name)
            self._add_flush_to_secdb(strategy)
        else:
            strategy = res[0]
            strategy.name = name
            strategy.event = event
            strategy.event_name = event_name
            self._flush_changes()
        return strategy


    def add_order(self, strategy_id, bet_id, size, side, selection_id,
                  price,executed,average_price, ref, state,market_id):
        res = get_session().query(Orders) \
            .filter(Orders.strategy_id == strategy_id, Orders.bet_id == bet_id) \
            .all()

        if len(res) == 0:
            order = Orders(strategy_id = strategy_id, bet_id = bet_id, size = size, side = side,
                                selection_id = selection_id,price = price,executed = executed,
                                ref = ref, average_price = average_price, state = state,market_id = market_id)
            self._add_flush_to_secdb(order)
        else:
            order = res[0]
            order.strategy_id = strategy_id
            order.bet_id = bet_id
            order.size = size
            order.side = side
            order.selection_id = selection_id
            order.price = price
            order.executed = executed
            order.average_price = average_price
            order.ref = ref
            order.state = state
            order.market_id = market_id
            self._flush_changes()

        return order

    @staticmethod
    def safe_str(s):
        return s.replace('’', "").replace("ł", "l").encode('ascii', 'ignore').decode('ascii')

class DBUnderGoalManager():
    def __init__(self, event_type, runner_query):
        self.event_type = event_type
        self.runner_query = runner_query

    def get_active_events(self, not_Traded = False):
        results = self.get_joined_tables_query()\
            .filter(EventTypes.name == self.event_type)\
            .filter(Markets.market_name.like(self.runner_query))\
            .filter(Markets.market_start_time > datetime.utcnow())\
            .order_by(Markets.market_start_time.asc())\
            .first()
        return results

    def get_joined_tables_query(self):
        return get_session().query(RunnersMap)\
            .join(Runners, Runners.selection_id == RunnersMap.selection_id)\
            .join(Markets, Markets.market_id == RunnersMap.market_id)\
            .join(Events, Events.event_id == Markets.event_id)\
            .join(Competitions, Events.competition_id == Competitions.competition_id)\
            .join(EventTypes, EventTypes.event_type_id == Competitions.event_type_id)


class RunnerMapQuery():
    def __init__(self):
        pass

    def get_events(self, event_filter="%%", competition_filter = "%%"):
        results = get_session().query(Events).filter(Events.name.like(event_filter))\
                    .filter(Competitions.name.like(competition_filter))\
                    .all()
        return results

    def get_markets(self, market_filter = "%%", event_filter="%%", competition_filter="%%"):
        results = get_session().query(Markets).join(Events, Events.event_id == Markets.event_id)\
            .join(Competitions, Events.competition_id == Competitions.competition_id)\
            .filter(Markets.market_name.like(market_filter)) \
            .filter(Events.name.like(event_filter)) \
            .filter(Competitions.name.like(competition_filter)) \
            .all()
        return results

    def get_runner_maps(self, competition, event, market_name_text, runner_name):
        results = self.get_joined_tables_query()\
            .filter(Competitions.name.like(competition))\
            .filter(Events.name.like(event))\
            .filter(Markets.market_name.like(market_name_text))\
            .filter(Runners.runner_name.like(runner_name))\
            .order_by(Markets.market_start_time.asc())\
            .all()
        return results

    def get_joined_tables_query(self):
        return get_session().query(RunnersMap, Runners, Markets, Events, Competitions)\
            .join(Runners, Runners.selection_id == RunnersMap.selection_id)\
            .join(Markets, Markets.market_id == RunnersMap.market_id)\
            .join(Events, Events.event_id == Markets.event_id)\
            .join(Competitions, Events.competition_id == Competitions.competition_id)\
            .join(EventTypes, EventTypes.event_type_id == Competitions.event_type_id)
