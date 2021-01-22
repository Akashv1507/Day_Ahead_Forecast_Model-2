import pandas as pd
import datetime as dt
from typing import List, Tuple, Union
import numpy as np
from src.fetchers.scadaApiFetcher import ScadaApiFetcher



def toMinuteWiseData(demandDf:pd.core.frame.DataFrame, entity:str)->pd.core.frame.DataFrame:
    """convert random secondwise demand dataframe to minwise demand dataframe and add entity column to dataframe.

    Args:
        demandDf (pd.core.frame.DataFrame): random secondwise demand dataframe
        entity (str): entity name

    Returns:
        pd.core.frame.DataFrame: minwise demand dataframe
    """    
    try:
        demandDf = demandDf.resample('1min', on='timestamp').agg({'demandValue': 'first'})  # this will set timestamp as index of dataframe
    except Exception as err:
        print('error while resampling', err)
    demandDf.insert(0, "entityTag", entity)                      # inserting column entityName with all values of 96 block = entity
    demandDf.reset_index(inplace=True)
    return demandDf

def toBlockWiseData(demandDf:pd.core.frame.DataFrame, entity:str)->pd.core.frame.DataFrame:
    """convert minwise demand dataframe to blockwise demand dataframe and add entity column to dataframe.

    Args:
        demandDf (pd.core.frame.DataFrame): minwise demand dataframe
        entity (str): entity name

    Returns:
        pd.core.frame.DataFrame: blockwise demand dataframe
    """    
    try:
        demandDf = demandDf.resample('15min', on='timestamp').mean()  # this will set timestamp as index of dataframe
    except Exception as err:
        print('error while resampling', err)
    demandDf.insert(0, "entityTag", entity)                      # inserting column entityName with all values of 96 block = entity
    demandDf.reset_index(inplace=True)
    return demandDf

def filterAction(demandDf :pd.core.frame.DataFrame, h1:int, h2:int, lowerBound:int, upperBound:int)-> pd.core.frame.DataFrame:
    """applying filtering to df by setting hyper parameters h1, h2.

    Args:
        demandDf (pd.core.frame.DataFrame): unfiltered df
        h1 (int): threshold hyper parameter h1
        h2 (int): window size hyper parameter h2
        lowerBound (int): lower bound demand value
        upperBound (int): upper Bound Demand value

    Returns:
        pd.core.frame.DataFrame: filtered dataframe df
    """    
    threshold = h1
    windowSize = h2
    #Applying hard boundaries to data 
    hardFilterMask = (demandDf['demandValue']>upperBound) | (demandDf['demandValue']<lowerBound)
    demandDf.loc[hardFilterMask,'demandValue']=np.nan
    
    # setting timestamp column to index
    demandDf.set_index('timestamp', inplace= True)
    # demandDf.to_excel(r'D:\wrldc_projects\AI_based_demand_forecasting\pandasToExcel\before-filtering.xlsx')
    #filling hard filtered outliers using time interpolation
    demandDf['demandValue'] = demandDf['demandValue'].interpolate(method= "time").ffill().bfill()
    
    demandDf["Spikes"] = demandDf["demandValue"].rolling(window=windowSize, center= True).median().fillna(method="bfill").fillna(method="ffill")
    
    #setting hyperparameter h1,h2
    demandDf['diff'] = np.abs(demandDf["demandValue"] - demandDf["Spikes"])
    rollingMedianMask= demandDf['diff']> threshold
    demandDf.loc[rollingMedianMask,'demandValue']= np.nan

    #filling outliers with time interpolation
    # print(demandDf['demandValue'].isna().sum())
    demandDf['demandValue'] = demandDf['demandValue'].interpolate(method= "time").ffill().bfill()
    # demandDf.to_excel(r'D:\wrldc_projects\AI_based_demand_forecasting\pandasToExcel\after-filtering.xlsx')
    demandDf.reset_index(inplace=True)
    return demandDf
    
    
def applyFilteringToDf(demandDf : pd.core.frame.DataFrame, entity:str)-> pd.core.frame.DataFrame:
    """ apply filtering logic to each entity demand data and returns df 

    Args:
        demandDf (pd.core.frame.DataFrame): demand dataframe
        entity (str): entity name

    Returns:
        filtered dataframe.
    """    
    if entity == 'WRLDCMP.SCADA1.A0046945':
        filteredDf = filterAction(demandDf,0,0,0,0)

    if entity == 'WRLDCMP.SCADA1.A0046948' or entity == 'WRLDCMP.SCADA1.A0046962' or entity == 'WRLDCMP.SCADA1.A0046953':
        filteredDf = filterAction(demandDf,0,0,0,0)
    
    if entity == 'WRLDCMP.SCADA1.A0046957' or entity == 'WRLDCMP.SCADA1.A0046978' or entity == 'WRLDCMP.SCADA1.A0046980':
        filteredDf = filterAction(demandDf,0,0,0,0)
   
    if entity == 'WRLDCMP.SCADA1.A0047000':
        filteredDf = filterAction(demandDf, 550, 3, 32775, 63000)
        
    return filteredDf

def toListOfTuple(df:pd.core.frame.DataFrame) -> List[Tuple]:
    """convert demand data to list of tuples

    Args:
        df (pd.core.frame.DataFrame): demand data dataframe

    Returns:
        List[Tuple]: list of tuple of demand data
    """    
    data:List[Tuple] = []
    for ind in df.index:
        tempTuple = (str(df['timestamp'][ind]), df['entityTag'][ind], float(df['demandValue'][ind]) )
        data.append(tempTuple)
    return data


def fetchDemandDataFromApi(currDate: dt.datetime, configDict: dict)-> List[Union[dt.datetime, str, float]]:
    """fetches demand data from api-> passes to filtering pipeline->resample to blockwise->generate list of tuple

    Args:
        currDate (dt.datetime): currant date
        configDict (dict): application dictionary

    Returns:
        dict: demand_purity_dict['data'] = per min demand data for each entity in form of list of tuple
              demand_purity_dict['purityPercentage'] = purity percentage of each entity in form of list of tuple

    """    
    tokenUrl: str = configDict['tokenUrl']
    apiBaseUrl: str = configDict['apiBaseUrl']
    clientId = configDict['clientId']
    clientSecret = configDict['clientSecret']

    
    #initializing temporary empty dataframe that append demand values of all entities
    storageDf = pd.DataFrame(columns = [ 'timestamp','entityTag','demandValue']) 

    #list of all entities
    # listOfEntity =['WRLDCMP.SCADA1.A0046945','WRLDCMP.SCADA1.A0046948','WRLDCMP.SCADA1.A0046953','WRLDCMP.SCADA1.A0046957','WRLDCMP.SCADA1.A0046962','WRLDCMP.SCADA1.A0046978','WRLDCMP.SCADA1.A0046980','WRLDCMP.SCADA1.A0047000']
    listOfEntity =['WRLDCMP.SCADA1.A0047000']

    
    #creating object of ScadaApiFetcher class 
    obj_scadaApiFetcher = ScadaApiFetcher(tokenUrl, apiBaseUrl, clientId, clientSecret)

    for entity in listOfEntity:
        # fetching secondwise data from api for each entity(timestamp,value) and converting to dataframe
        resData = obj_scadaApiFetcher.fetchData(entity, currDate, currDate)
        demandDf = pd.DataFrame(resData, columns =['timestamp','demandValue']) 

        #converting to minutewise data and adding entityName column to dataframe
        demandDf = toMinuteWiseData(demandDf,entity)
       
        #applying filtering logic
        filteredDf = applyFilteringToDf(demandDf,entity)
        # filteredDf.to_excel(r'D:\wrldc_projects\demand_forecasting\filtering demo\filtered_Wr_dec_jan.xlsx')
        #converting to blockwise demand data and adding entityName column to dataframe
        blockwiseDf = toBlockWiseData(filteredDf,entity)

        #appending per min demand data for each entity to tempDf
        storageDf = pd.concat([storageDf, blockwiseDf],ignore_index=True)

    
    # converting storageDf(contain per min demand values of all entities) to list of tuple 
    data:List[Tuple] = toListOfTuple(storageDf)
    
    return data
    