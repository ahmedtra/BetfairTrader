from sklearn.externals import joblib
from  structlog import get_logger

class RFRPredictor():
    def __init__(self, pred_file, encoder_file, runners, stake = 4, target_restrict = None, 
                 scale_with_pred=False, scale_with_odds=False, min_odds = 1.01,
                 max_odds = 1000, min_pred = 0):
        
        self.encoder_file = encoder_file
        self.pred_file = pred_file
        self.runners = runners
        self.stake = stake
        self.target_restrict = target_restrict
        self.scale_with_pred = scale_with_pred
        self.scale_with_odds = scale_with_odds
        self.min_odds = min_odds
        self.max_odds = max_odds
        self.min_pred = min_pred
        self.models = self.read_models(self.pred_file, self.runners)
        self.encoder = self.read_encoder(self.encoder_file)
        self.pred = [0 for r in self.runners]
        
    def read_models(self, file, runners):
        models = {}
        for runner in runners:
            models[runner] = joblib.load(file+"_"+runner+".pkl")
        return models

    def read_encoder(self, file):
        return joblib.load(file+".pkl")

    def make_prediction(self, data):
        
        try:
            team1_label = self.encoder.transform([data["team1"]])[0]
            team2_label = self.encoder.transform([data["team2"]])[0]
        except:
            self.pred = [-1000 for r in self.runners]
            return {runner:-1000 for runner in self.runners}
        
        regressors = [[team1_label,team2_label,data["1"], data["x"], data["2"]]]

        pred = {}

        for runner in self.runners:
            pred[runner] = self.models[runner].predict(regressors)[0]

        self.pred = [pred[r] for r in self.runners]

        return pred

    def get_bet(self, odds):
        
        pred = self.make_prediction(odds)
        
        target = self.runners[0]
        for runner in self.runners:
            if float(pred[runner]) > float(pred[target]):
                target = runner

        restricted = self.target_restrict is None or target in self.target_restrict
        try:
            get_logger().info("data", team_1 = odds["team1"], team_2 = odds["team2"], odd_1 = odds["1"]
                          , odd_x=odds["x"], odd_2 = odds["2"])
        except:
            get_logger().info("data", odds = odds)
        get_logger().info("prediction", pred=pred)

        if pred[target] > self.min_pred and float(odds[target])<self.max_odds \
                    and float(odds[target])>self.min_odds and restricted:
            
            stake_adjusted = self.stake

            if self.scale_with_odds:
                stake_adjusted = stake_adjusted / float(odds[target])

            if self.scale_with_pred:
                stake_adjusted = stake_adjusted * max(min(pred[target], 5), 1)
            return target, stake_adjusted
        else:
            return target, 0

    def get_pred(self):
        return self.pred