
class Account:

    def __init__(self, account_number, keys, amount, start, end, metrics):
        
        self.account_number = account_number
        self.keys = keys  
        self.amount = amount      
        self.start = start
        self.end = end
        self.metrics = metrics
        self.forecast_mean_value = 0
        self.forecast_prediction_interval_lowerbound = 0    
        self.forecast_prediction_interval_upperbound = 0    
