from authenticate import authenticate
from list_team import team_list, over_under

client = authenticate()

from betfair.models import MarketFilter
event_types = client.list_event_types(MarketFilter(event_type_ids = []))
print(len(event_types))

for et in event_types:
    print(et.event_type.name)
    print(et.event_type.id)
  # 'Tennis'
soccer_type_ids = [1]
all_over_under = ','.join(over_under)
market_porjection = ["COMPETITION", "MARKET_START_TIME", "RUNNER_DESCRIPTION", "EVENT"]
markets = client.list_market_catalogue(
    MarketFilter(event_type_ids=soccer_type_ids, event_types = team_list, text_query = "OVER_UNDER_*5"),
    market_projection=market_porjection,
    max_results = 1000
)

print("here")
#book = client.list_market_book(market_ids=)



for market in markets:
    market._data["event_name"] = market._data["event"]["name"]

import pandas as pd

market_df = pd.DataFrame([market._data for market in markets])

market_df.to_clipboard()
print(markets)