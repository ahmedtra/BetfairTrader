from collections import namedtuple

FIELDS_Quote= ('market_id','selection_id', 'status', 'timestamp',
               'total_matched', 'last_price_traded', 'inplay',
               'back_1', 'back_size_1','back_2', 'back_size_2','back_3', 'back_size_3',
               'lay_1', 'lay_size_1', 'lay_2', 'lay_size_2','lay_3', 'lay_size_3')

Quote = namedtuple('Quote', FIELDS_Quote)
