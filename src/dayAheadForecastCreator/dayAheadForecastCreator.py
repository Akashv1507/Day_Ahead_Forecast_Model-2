import datetime as dt
from typing import List, Tuple, Union
import pandas as pd
from src.dayAheadForecastCreator.blockwiseDemandFetch import DemandFetchForModelRepo
from src.dayAheadForecastCreator.mlrPredictions import MlrPredictions
from src.dayAheadForecastCreator.daForecastInsertion import DayAheadDemandForecastInsertion



def createDayAheadForecast(startDate:dt.datetime ,endDate: dt.datetime, configDict:dict)->bool:
    """ create DA forecast using DFM-2
    Args:
        startDate (dt.datetime): start date
        endDate (dt.datetime): end date
        configDict (dict):   apllication configuration dictionary
    Returns:
        bool: return true if insertion is success.
    """    

    
    conString:str = configDict['con_string_mis_warehouse']
    modelPath:str = configDict['model_path']
    # listOfEntity =['WRLDCMP.SCADA1.A0046945','WRLDCMP.SCADA1.A0046948','WRLDCMP.SCADA1.A0046953','WRLDCMP.SCADA1.A0046957','WRLDCMP.SCADA1.A0046962','WRLDCMP.SCADA1.A0046978','WRLDCMP.SCADA1.A0046980','WRLDCMP.SCADA1.A0047000']
    listOfEntity =['WRLDCMP.SCADA1.A0047000', 'WRLDCMP.SCADA1.A0046978','WRLDCMP.SCADA1.A0046980', 'WRLDCMP.SCADA1.A0046957']
    # listOfEntity =['WRLDCMP.SCADA1.A0046957']

    
    #creating instance of class
    obj_demandFetchForModelRepo = DemandFetchForModelRepo(conString)
    obj_mlrPredictions = MlrPredictions(modelPath)
    obj_daDemandForecastInsertion = DayAheadDemandForecastInsertion(conString)
    
    #intializing empty dataframe to store forecast of all entities
    storeForecastDf = pd.DataFrame(columns = [ 'timestamp','entityTag','forecastedDemand']) 
    insertSuccessCount=0
    currDate = startDate
    
    # Iterating through each day and each entities , storing DA forecast in storeForecastDf anf psuhing into db 
    while currDate <= endDate:
        for entity in listOfEntity:
            if entity =='WRLDCMP.SCADA1.A0047000':
                lagDemandDf = obj_demandFetchForModelRepo.fetchBlockwiseDemandForModel(currDate, entity, lagStart=0)
            elif entity == 'WRLDCMP.SCADA1.A0046978':
                lagDemandDf = obj_demandFetchForModelRepo.fetchBlockwiseDemandForModel(currDate, entity, lagStart=1)
            elif entity == 'WRLDCMP.SCADA1.A0046980':
                lagDemandDf = obj_demandFetchForModelRepo.fetchBlockwiseDemandForModel(currDate, entity, lagStart=0)
            elif entity == 'WRLDCMP.SCADA1.A0046957':
                lagDemandDf = obj_demandFetchForModelRepo.fetchBlockwiseDemandForModel(currDate, entity, lagStart=0)
            predictedDaDf = obj_mlrPredictions.predictDaMlr(lagDemandDf, entity)
            # print(predictedDaDf)
            storeForecastDf = pd.concat([storeForecastDf, predictedDaDf],ignore_index=True)

        isInsertionSuccess =  obj_daDemandForecastInsertion.insertDayAheadDemandForecast(storeForecastDf)

        if isInsertionSuccess:
            insertSuccessCount = insertSuccessCount + 1
        currDate += dt.timedelta(days=1)
    
    numOfDays = (endDate-startDate).days

    #checking whether data is inserted for each day or not
    if insertSuccessCount  == numOfDays +1:
        return True
    else:
        return False
    