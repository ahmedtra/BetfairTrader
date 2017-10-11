from os.path import join

from common import initialize_logging, ROOT_DIR
from predictors.RFRPredictor import RFRPredictor
from strategy_handlers.strategies.MLPredictor import MLPredictor
from strategy_handlers.strategies_manager import strategy_manager

if __name__ == "__main__":
    initialize_logging("machine_learning_predictor")

    path_models = join(ROOT_DIR, "predictors\RFPPredictorModels\predictor")
    path_encoder = join(ROOT_DIR, "predictors\RFPPredictorModels\encoder")
    runners = ["1","x","2"]
    min_odds = 1.1
    max_odds = 4
    min_pred = 0.5
    scale_with_pred = True
    stake = 4

    predictor = RFRPredictor(path_models,path_encoder, runners, stake = stake, scale_with_pred=scale_with_pred,
                             min_odds=min_odds, max_odds= max_odds, min_pred= min_pred)
    sm = strategy_manager(MLPredictor, number_threads=1, predictor = predictor,
                          max_odds = max_odds, min_odds = min_odds)
    sm.manage_strategies()