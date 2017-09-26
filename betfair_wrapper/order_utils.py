from betfair.constants import PriceData, OrderType, PersistenceType, RollupModel
from betfair.models import PriceProjection, PlaceInstruction, LimitOrder, ReplaceInstruction, \
    CancelInstruction, ExBestOffersOverrides

soccer_type_ids = [1]




def place_bet(client, price, size, side, market_id, selection_id):
    size = round(size, 2)
    size_reduction = 0
    if size < 4:
        size_reduction = 4 - size

    order = PlaceInstruction()
    order.order_type = OrderType.LIMIT
    order.selection_id = selection_id
    order.side = side
    limit_order = LimitOrder()
    limit_order.price = price
    limit_order.size = max(size, 4)
    limit_order.persistence_type = PersistenceType.LAPSE
    order.limit_order = limit_order

    instructions = [order]

    response = client.place_orders(market_id, instructions)

    match = {}
    match["bet_id"] = response.instruction_reports[0].bet_id
    match["price"] = response.instruction_reports[0].average_price_matched
    match["size"] = response.instruction_reports[0].size_matched
    if size_reduction > 0:
        bet_id = match["bet_id"]
        cancel_order(client, market_id, bet_id, size_reduction)

    return match


def cancel_order(client, market_id, bet_id, size_reduction=None):
    if size_reduction is not None:
        size_reduction = round(size_reduction, 2)
    instruction_cancel = CancelInstruction()
    instruction_cancel.bet_id = bet_id
    instruction_cancel.size_reduction = size_reduction

    response = client.cancel_orders(market_id, [instruction_cancel])


def replace_order(client, market_id, bet_id, new_price):
    instruction_update = ReplaceInstruction()
    instruction_update.bet_id = bet_id
    instruction_update.new_price = new_price

    response = client.replace_orders(market_id, [instruction_update])

    match = {}

    match["bet_id"] = response.instruction_reports[0].place_instruction_report.bet_id
    match["price"] = response.instruction_reports[0].place_instruction_report.average_price_matched
    match["size"] = response.instruction_reports[0].place_instruction_report.size_matched

    return match


def get_price_market_selection(client, market_id, selection_id):
    price_projection = PriceProjection()
    price_projection.price_data = [PriceData.EX_BEST_OFFERS]
    price_projection.virtualise = True
    price_projection.rollover_stakes = True
    ex_best_offers_overrides = ExBestOffersOverrides()
    ex_best_offers_overrides.best_prices_depth = 3
    ex_best_offers_overrides.rollup_model = RollupModel.STAKE
    ex_best_offers_overrides.rollup_limit = 1
    price_projection.ex_best_offers_overrides = ex_best_offers_overrides
    books = client.list_market_book(market_ids=[market_id], price_projection=price_projection)

    for runner in books[0].runners:
        if runner.selection_id == selection_id:
            if len(runner.ex.available_to_back) == 0:
                return None, None, None, None, None
            back = runner.ex.available_to_back[0].price
            lay = None
            if len(runner.ex.available_to_lay) != 0:
                lay = runner.ex.available_to_lay[0].price
            size = runner.ex.available_to_back[0].size
            orders = runner.orders
            status = runner.status
            if books[0].status == "SUSPENDED":
                status = "SUSPENDED"
            return back, lay, size, status, orders
    return None, None, None, None, None


def get_placed_orders(client, market_ids):
    response = client.list_current_orders(market_ids=market_ids)

    return response.current_orders
