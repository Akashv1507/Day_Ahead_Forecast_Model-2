import datetime as dt
from typing import List, Tuple, Union
from src.filteredScadaDemandTodb.demandDataFetcher import fetchDemandDataFromApi
from src.filteredScadaDemandTodb.filteredBlockwiseDemandToDb import InterpolatedBlockWiseDemandInsRepo



def insFilteredScadaDemand(startDate:dt.datetime ,endDate: dt.datetime, configDict:dict)->bool:
    """ push raw scada data to db after passing through filtering pipeline
    Args:
        startDate (dt.datetime): start date
        endDate (dt.datetime): end date
        configDict (dict):   apllication configuration dictionary
    Returns:
        bool: return true if insertion is success.
    """    

    
    conString:str = configDict['con_string_mis_warehouse']

    #creating instance of class
    obj_interpolatedBlockwiseDemandInsRepo = InterpolatedBlockWiseDemandInsRepo(conString)
    
    insertSuccessCount=0
    currDate = startDate
    
    # Iterating through each day and inserting filtered scada data 
    while currDate <= endDate:
        data:List[Union[dt.datetime, str, float]] = fetchDemandDataFromApi(currDate, configDict)
        # print(data)
        isInsertionSuccess = obj_interpolatedBlockwiseDemandInsRepo.insertBlockWiseDemand(data)
        if isInsertionSuccess:
            insertSuccessCount = insertSuccessCount + 1
        currDate += dt.timedelta(days=1)
    
    numOfDays = (endDate-startDate).days

    #checking whether data is inserted for each day or not
    if insertSuccessCount  == numOfDays +1:
        return True
    else:
        return False
    