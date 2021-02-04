import pandas as pd 
import datetime as dt
import joblib


class MlrPredictions():
    """MLR prediction class
    """

    def __init__(self, modelPath: str) -> None:
        """load prediction model path
        Args:
            modelPath ([type]): path of model
        """
        self.modelPath = modelPath
        self.modelPathStr =""

    def dummyVariableGenerator(self, ts):
        #Making dummy variables
        month = pd.Series(ts.index.month.astype(str), index=ts.index, name="month").apply(lambda x: "m{}".format(x)) #m1 -> January
        day = pd.Series(ts.index.dayofweek.astype(str), index=ts.index, name="day").apply(lambda x: "d{}".format(x)) #d0 -> Monday
        timeblock = pd.Series(ts.index.strftime("%H:%M"), index=ts.index, name="timeblock")
        daytimeblock = (day + "_" + timeblock).rename("daytimeblock")
        #Making dummy variables
        monthDummies = pd.get_dummies(month.sort_values()).sort_index()
        # dayDummies = pd.get_dummies(day.sort_values()).sort_index()
        # hourDummies = pd.get_dummies(hour.sort_values()).sort_index()
        daytimeblockDummies = pd.get_dummies(daytimeblock.sort_values()).sort_index()
        return monthDummies, daytimeblockDummies
        
    def modelPredictions(self, lagDemandDf, monthDummies, daytimeblockDummies):
             
        prediction_obj = joblib.load(self.modelPathStr)
        X_input = pd.concat([monthDummies.iloc[:,:-1],  #Exclude the last category
                            daytimeblockDummies.iloc[:,:-1],  #Exclude the last category
                            lagDemandDf   
                            ], axis=1, join= "inner")
        # print(X_input)
        X_input_arr = X_input.values
        Y_pred = pd.Series(prediction_obj.predict(X_input_arr).flatten(), 
                        index= pd.DatetimeIndex(X_input.index) + pd.DateOffset(0), name= "Y_pred")
        return Y_pred

    def predictDaMlr(self, lagDemandDf:pd.core.frame.DataFrame, entity:str)-> pd.core.frame.DataFrame:
        """predict DA forecast using model based on entity

        Args:
            lagDemandDf (pd.core.frame.DataFrame): dataframe containing blockwise D-2, D-7, D-14, D-21 demand with index timestamp of 'D'
            entity (str): entity tag like 'WRLDCMP.SCADA1.A0047000'

        Returns:
            pd.core.frame.DataFrame: DA demand forecast with column(timestamp, entityTag, demandValue)
        """    

        #setting model path string(class variable) based on entity tag(means deciding which model ti use)
        self.modelPathStr = self.modelPath + '\\' + str(entity) +'.pkl'

        ts = pd.date_range(start = pd.Timestamp("2021-01-01 00:00:00"), end = pd.Timestamp("2021-12-31 23:59:59"),
                               freq ='15min').rename("time").to_frame()
        monthDummies, dayhourDummies = self.dummyVariableGenerator(ts)

        daPredictionSeries= self.modelPredictions(lagDemandDf, monthDummies,dayhourDummies)
        daPredictionDf = daPredictionSeries.to_frame()

        #adding entityTag column and resetting index
        daPredictionDf.insert(0, "entityTag", entity)  
        daPredictionDf.reset_index(inplace=True)

        #renaming columns
        daPredictionDf.rename(columns={'index': 'timestamp', 'Y_pred': 'forecastedDemand'}, inplace=True)
        
        return daPredictionDf

