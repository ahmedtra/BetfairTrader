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

FIELDS_Trades= ('sports_id', 'event_id', 'settled_date', 'full_description', 'scheduled_off',
                'event', 'dt_actual_off', 'selection_id', 'selection', 'odds', 'number_bets',
                'volume_matched', 'latest_taken', 'first_taken', 'win_flag', 'in_play',
                'competition_type', 'competition', 'fixtures', 'event_name', 'market_type')

Trades = namedtuple('Trades', FIELDS_Trades)

