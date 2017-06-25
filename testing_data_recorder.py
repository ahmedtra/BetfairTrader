from data.quote_recorder import Recorder
from authenticate import authenticate
from list_team import team_list

client = authenticate()

dr = Recorder(client, [1])

dr.update_market_list()

dr.get_runner_prices_and_save()