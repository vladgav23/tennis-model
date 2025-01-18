import logging
from flumine.markets.middleware import Middleware

logger = logging.getLogger(__name__)

class GetPricesFromScoredHoldout(Middleware):
    def __init__(self, preds_df):
        self.preds_df = preds_df

    def add_market(self, market) -> None:
        market.context['preds'] = self.preds_df.query('market_id == @market.market_id').to_dict('records')