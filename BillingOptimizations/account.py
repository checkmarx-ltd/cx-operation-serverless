
class Account:

    def __init__(self, pu, account_number, keys, amount, start, end, metrics, account_name):


        self.pu = pu
        self.account_name = account_name
        self.account_number = account_number
        self.keys = keys  
        self.amount = amount      
        self.start = start
        self.end = end
        self.metrics = metrics
        self.forecast_mean_value = 0
        self.forecast_prediction_interval_lowerbound = 0    
        self.forecast_prediction_interval_upperbound = 0  

    @staticmethod
    def map_pu_to_account (account_number):

        if account_number in ("379959622371", "267564311969", "759786165482", "771250916586"):
            return "CB"
        elif account_number in ("656509302511"):
            return "Operation"
        elif account_number in ("359856697693", "544428519067", "425154196092", "238450497947", "185449903594", "092796063756"):
            return "SAST"        
        elif account_number in ("983555762431", "148244689188"):
            return "AST Integration"
        elif account_number in ("881053136306"):
            return "Architecture"
        elif account_number in ("666740670058", "006765415138"):
            return "SCA"
        elif account_number in ("941355383184","263675485306"):
            return "AST"
        elif account_number in ("866472756642"):
            return "IAST"
        elif account_number in ("002023477301","098770948573","844024982327","074002600390","185216882498","715799477975","748129348851","938742211667","457489196639","354891787287"):
             return "CxGo"
        else:
            return "Other"


    @staticmethod
    def map_account_name_to_account_number (account_number):
        if account_number == "379959622371":
            return "CodeBashingSharedSvc"
        elif account_number == "941355383184":
            return "Cx-Aws-Ast-Dev"
        elif account_number == "267564311969":
            return "CxAwsCodeBashing"
        elif account_number == "759786165482":
            return "CxAwsCodeBashingDevelopment"
        elif account_number == "771250916586":
            return "CxAwsCodeBashingStaging"
        elif account_number == "881053136306":
            return "CxAwsDataCollectionRnD" 
        elif account_number == "656509302511":
            return "CxAwsDevOps"       
        elif account_number == "666740670058":
            return "CxAWSLumoDev"
        elif account_number == "006765415138":
            return "CxAWSLumoPPE"
        elif account_number == "148244689188":
            return "CxAwsMandODev"
        elif account_number == "983555762431":
            return "cxflowci"
        elif account_number == "263675485306":
            return "CxAST-TS"        
        elif account_number == "359856697693":
            return "CxSastAccessControlDev"        
        elif account_number == "092796063756":
            return "CxSastAccessControlProductionBeta"        
        elif account_number == "544428519067":
            return "CxSastAccessControlSharedServices"
        elif account_number == "425154196092":
            return "CxSastAccessControlStaging"
        elif account_number == "238450497947":
            return "CxSastDev"
        elif account_number == "185449903594":
            return "CxSastDevTeam17"   
        elif account_number == "866472756642":
            return "CxAWSIastDev"                 
        elif account_number == "002023477301":
            return "MCSSsa"
        elif account_number == "098770948573":
            return "MCSPPE"
        elif account_number == "844024982327":
            return "MCSPPEReplica"
        elif account_number == "074002600390":
            return "MCSSandbox"
        elif account_number == "185216882498":
            return "MCSProdReplica"
        elif account_number == "715799477975":
            return "MCSDev"
        elif account_number == "748129348851":
            return "MCSFeatureOne"
        elif account_number == "938742211667":
            return "MCSFeatureTwo"
        elif account_number == "457489196639":
            return "MCSFeatureThree"
        elif account_number == "354891787287":
            return "MCSHSBCReplica"              
        else:
            return "Other"