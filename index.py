import argparse
import datetime as dt
from src.appConfig import getAppConfigDict
from src.rawDataCreator.minWiseDemand_purtiyPercentCreator import createMinWiseDemand_purityPercent
from src.rawDataCreator.entityMappingTableCreator import createEntityMappingTable
from src.derivedDataCreator.blockwiseDemandCreator import createBlockWiseDemand
from src.derivedDataCreator.stagingBlockwiseDemandCreator import createStagingBlockWiseDemand
from src.utils.checkSpecialDay import checkSpecialDay
from src.adjustmentBeforeForecast import doAdjustmentBeforeForecast
from src.derivedDataCreator.dayAheadForecastCreator import createDayAheadForecast

startDate = dt.datetime.strptime("2020-08-25", '%Y-%m-%d')
endDate = dt.datetime.strptime("2020-09-08", '%Y-%m-%d')

#command line i/p default set to false
wantUpdation = True

# 
configDict=getAppConfigDict()
print(createMinWiseDemand_purityPercent(startDate,endDate,configDict))

# createEntityMappingTable(configDict)
print(createBlockWiseDemand(startDate,endDate,configDict))

print(createStagingBlockWiseDemand(startDate,endDate,configDict,wantUpdation))

# doAdjustmentBeforeForecast(startDate,startDate,configDict)
# createDayAheadForecast(startDate,startDate,configDict)

# x  = 6 in [1,2,3,4,5]
# print(x)