import cx_Oracle
import pandas as pd
import datetime as dt
from typing import List, Tuple, TypedDict

class DemandFetchForModelRepo():
    """fetch blockwise D-2, D-7, D-14, D-21 demand and return dataframe of it.
    """

    def __init__(self, con_string):
        """initialize connection string
        Args:
            con_string ([type]): connection string 
        """
        self.connString = con_string
         
         

    def fetchBlockwiseDemandForModel(self, currDateKey: dt.datetime, entity:str) -> pd.core.frame.DataFrame:
        """"fetch blockwise D-2, D-7, D-14, D-21 demand and return dataframe of it.
        Args:
            self: object of class 
            currDateKey (dt.datetime): start-date
            entity(str): entity tag like 'WRLDCMP.SCADA1.A0047000'
        Returns:
            pd.core.frame.DataFrame: dataframe containing blockwise D-2, D-7, D-14, D-21 demand with index timestamp of 'D'
        """
        dMinus2 = currDateKey-dt.timedelta(days=1)
        dMinus7 = currDateKey-dt.timedelta(days=6)
        dMinus14 = currDateKey-dt.timedelta(days=13)
        dMinus21 = currDateKey-dt.timedelta(days=20)
        # print(dMinus2,dMinus7,dMinus14,dMinus21)

        dMinus2_startTime = dMinus2
        dMinus2_endTime = dMinus2 + dt.timedelta(hours= 23,minutes=45)
        dMinus7_startTime = dMinus7
        dMinus7_endTime = dMinus7 + dt.timedelta(hours= 23,minutes=45)
        dMinus14_startTime = dMinus14
        dMinus14_endTime = dMinus14 + dt.timedelta(hours= 23,minutes=45)
        dMinus21_startTime = dMinus21
        dMinus21_endTime = dMinus21 + dt.timedelta(hours= 23,minutes=45)
        

        try:
            connection = cx_Oracle.connect(self.connString)

        except Exception as err:
            print('error while creating a connection', err)
        else:
            try:
                cur = connection.cursor()
                fetch_sql = "SELECT time_stamp, demand_value FROM interpolated_blockwise_demand WHERE time_stamp BETWEEN TO_DATE(:start_time) and TO_DATE(:end_time) and entity_tag = :tag ORDER BY time_stamp"
                cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' ")
                dMinus2Df = pd.read_sql(fetch_sql, params={
                                    'start_time': dMinus2_startTime, 'end_time': dMinus2_endTime, 'tag': entity}, con=connection)
                dMinus7Df = pd.read_sql(fetch_sql, params={
                                    'start_time': dMinus7_startTime, 'end_time': dMinus7_endTime, 'tag': entity}, con=connection)
                dMinus14Df = pd.read_sql(fetch_sql, params={
                                    'start_time': dMinus14_startTime, 'end_time': dMinus14_endTime, 'tag': entity}, con=connection)
                dMinus21Df = pd.read_sql(fetch_sql, params={
                                    'start_time': dMinus21_startTime, 'end_time': dMinus21_endTime, 'tag': entity}, con=connection)
            # deleting timestamp column and renaming demand_value column of each df
                del dMinus2Df['TIME_STAMP']
                del dMinus7Df['TIME_STAMP']
                del dMinus14Df['TIME_STAMP']
                del dMinus21Df['TIME_STAMP']

                dMinus2Df.rename(columns = {'DEMAND_VALUE':'dMinus2DemandValue'}, inplace = True)
                dMinus7Df.rename(columns = {'DEMAND_VALUE':'dMinus7DemandValue'}, inplace = True)
                dMinus14Df.rename(columns = {'DEMAND_VALUE':'dMinus14DemandValue'}, inplace = True)
                dMinus21Df.rename(columns = {'DEMAND_VALUE':'dMinus21DemandValue'}, inplace = True) 
                #concatenating d-2,d-7,d-9,d-14 demand value of particular entity horizontaly
                # demandConcatDf = pd.concat([dMinus2Df,dMinus7Df,dMinus14Df,dMinus21Df], axis=1)
                demandConcatDf = pd.concat([dMinus21Df,dMinus14Df,dMinus7Df,dMinus2Df], axis=1)


                #generating timestamp column for date of forecast
                dateOfForecast = currDateKey + dt.timedelta(days=1)
                timestampValues = pd.date_range(start=dateOfForecast,freq='15min',periods=96)
                demandConcatDf.insert(0, "timestamp", timestampValues)  
                #setting timestamp as index
                demandConcatDf.set_index('timestamp', inplace= True)
                    
            except Exception as err:
                print('error while creating a cursor', err)
            else:
                connection.commit()
        finally:
            cur.close()
            connection.close()
        return demandConcatDf
        
       
