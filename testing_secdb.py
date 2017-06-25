from authenticate import authenticate
from data.sql_wrapper.connection import initialize_secdb, get_session
from data.sql_wrapper.model import Competitions
from data.db_recorder import DBRecorder
from data.sql_wrapper.query import DBUnderGoalManager, RunnerMapQuery

if __name__ == "__main__":
    client = authenticate()
    initialize_secdb()
    db_recorder = DBRecorder(client)
#    db_UGM = DBUnderGoalManager("Soccer", "%Over/Under%")
    #db_rm = RunnerMapQuery()
    db_recorder.fetch_event_types()
    #db_recorder.fetch_competitions()
    #db_recorder.fetch_events()
    #db_recorder.fetch_markets_runners()

    #all_over_under = db_rm.get_runner_maps("soccer_trades_hist_betfair", "%Over/Under%", "Over 0.5 Goals")

    print("here")