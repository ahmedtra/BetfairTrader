from betfair.constants import MarketProjection
from betfair.models import MarketFilter

from common import initialize_logging
from data.sql_wrapper.connection import initialize_secdb
from data.sql_wrapper.query import DBQuery

from structlog import get_logger

class DBRecorder():
    def __init__(self, client):
        initialize_secdb()
        initialize_logging("db_recorder")
        self.betfair_client = client
        self.db_client = DBQuery()

    def fetch_event_types(self):
        event_types = self.betfair_client.list_event_types()
        for event_type in event_types:
            id = event_type.event_type.id
            name = event_type.event_type.name
            market_count = event_type.market_count
            self.db_client.add_event_type(id, name, market_count)
        self.db_client.commit_changes()

    def fetch_competitions(self):
        event_types = self.db_client.get_event_types()
        for event_type in event_types:
            competitions = self.betfair_client.list_competitions(
                filter = MarketFilter(event_type_ids = [event_type.event_type_id]))
            for competition in competitions:
                id = competition.competition.id
                name = competition.competition.name
                region = competition.competition_region
                market_count = competition.market_count
                event_type_id = event_type.id
                self.db_client.add_competition(id, name, region, market_count, event_type_id)
        self.db_client.commit_changes()


    def fetch_events(self):
        competitions = self.db_client.get_competitions()
        for competition in competitions:
            events = self.betfair_client.list_events(
                filter = MarketFilter(competition_ids = [competition.competition_id]))
            for event in events:
                event_id = event.event.id
                name = event.event.name
                country_code = event.event.country_code
                timezone = event.event.timezone
                venue = event.event.venue
                open_date = event.event.open_date
                market_count = event.market_count
                competition_id = competition.id
                self.db_client.add_event(event_id, name, country_code, timezone,
                                         venue, open_date, market_count, competition_id)
        self.db_client.commit_changes()

    def fetch_markets_runners(self):
        events = self.db_client.get_events()
        for event in events:
            if event.id < 1271:
                continue
            markets = self.betfair_client.list_market_catalogue(
                filter = MarketFilter(event_ids = [event.event_id]),
                market_projection = ["RUNNER_DESCRIPTION", 'MARKET_DESCRIPTION', 'MARKET_START_TIME', 'RUNNER_METADATA']
            )
            for market in markets:
                market_id = market.market_id
                name = market.market_name
                description = market.description.betting_type
                market_start_time = market.market_start_time
                event_id = event.id
                get_logger().info("adding market", market_name = name, market_id = market_id,
                                  description = description, market_start_time = market_start_time,
                                  event_id = event_id)
                db_market = self.db_client.add_market(market_id, name, market_start_time, description,
                                          event_id)
                for runner in market.runners:
                    selection_id = runner.selection_id
                    runner_name = runner.runner_name
                    handicap = runner.handicap
                    sort_priority = runner.sort_priority
                    meta_data_id = None
                    db_runner = self.db_client.add_runner(selection_id, runner_name, handicap, sort_priority, meta_data_id)
                    if selection_id == 10084745:
                        pass

                    self.db_client.add_runner_map(db_market.id, db_runner.id)

                self.db_client.commit_changes()

