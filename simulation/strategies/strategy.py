import logging

import flumine.utils
import numpy as np
from flumine.markets.market import Market
from betfairlightweight.resources import MarketBook
from flumine import BaseStrategy
from flumine.utils import get_price
from flumine.order.trade import Trade
from flumine.order.order import LimitOrder

logger = logging.getLogger(__name__)

class TennisH2H(BaseStrategy):
    def __init__(self, stake_unit,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stake_unit = stake_unit

    def check_market_book(self, market: Market, market_book: MarketBook) -> bool:
        # Ignore closed or in-play markets
        if market_book.status != "CLOSED" and not market_book.inplay:
            return True

    def process_market_book(self, market: Market, market_book: MarketBook) -> None:
        if not market.context.get('preds'):
            return

        market_preds = market.context['preds'][0]
        model_preds = [val for key, val in market_preds.items() if 'low_sample' in key]
        back_prices = [{'sel': x.selection_id, 'atb': get_price(x.ex.available_to_back, 0)} for x in
                       market_book.runners]

        if sum([x['atb'] is not None for x in back_prices]) != 2:
            return

        for runner in market_book.runners:
            runner_is_home = runner.selection_id == market_preds['selection_id_home']
            runner_is_away = runner.selection_id == market_preds['selection_id_away']

            if not (runner_is_away or runner_is_home):
                return

            opponent_price = [x['atb'] for x in back_prices if x['sel'] != runner.selection_id]
            if not opponent_price:
                return

            if runner_is_away:
                model_preds = [1 - x for x in model_preds]

            best_back_price = get_price(runner.ex.available_to_back, 0)

            if not best_back_price:
                return

            best_back_size = flumine.utils.get_size(runner.ex.available_to_back, 0)

            # ofp_mean = sum(model_preds) / len(model_preds)
            ofp_min = min(model_preds)
            # pred_var = np.var(model_preds)

            ev = (ofp_min * (best_back_price - 1) * 0.95) - (1 - ofp_min)

            if 0.0 < ev:
                trade = Trade(
                    market_book.market_id,
                    runner.selection_id,
                    runner.handicap,
                    self
                )
                order = trade.create_order(
                    side="BACK",
                    order_type=LimitOrder(best_back_price,min(best_back_size,self.stake_unit))
                )
                market.place_order(order)
