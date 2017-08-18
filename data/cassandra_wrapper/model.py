from collections import namedtuple

FIELDS_Quote= ('market_id','selection_id', 'status', 'timestamp',
               'total_matched', 'last_price_traded', 'inplay',
               'back_1', 'back_size_1','back_2', 'back_size_2','back_3', 'back_size_3',
               'lay_1', 'lay_size_1', 'lay_2', 'lay_size_2','lay_3', 'lay_size_3')

Quote = namedtuple('Quote', FIELDS_Quote)


FIELDS_Trades_min= ("market_id","selection_id","status","timestamp",
                    "total_matched","last_price_traded","inplay","back_1",
                    "back_size_1","back_2","back_size_2","back_3",
                    "back_size_3","lay_1","lay_size_1","lay_2","lay_size_2","lay_3","lay_size_3")

Trades_min = namedtuple('Trades_min', FIELDS_Trades_min)

FIELDS_Trades= ("SPORTS_ID","EVENT_ID","SETTLED_DATE","FULL_DESCRIPTION","SCHEDULED_OFF",
                "EVENT","DT ACTUAL_OFF","SELECTION_ID","SELECTION","ODDS","NUMBER_BETS",
                "VOLUME_MATCHED","LATEST_TAKEN","FIRST_TAKEN","WIN_FLAG",
                "IN_PLAY","COMPETITION_TYPE","COMPETITION","FIXTURES","EVENT_NAME",
                "MARKET_TYPE",)

Trades = namedtuple('Trades', FIELDS_Trades)

