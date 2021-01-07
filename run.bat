call venv\Scripts\activate.bat
call python index_insMinwiseDemand.py
call python index_insBlockwiseDemand.py
call python index_insStagingBlockwiseDemand.py --want_updation True
REM forecast adjustment
call python index_adjustmentBeforeForecast.py
call python index_dayAheadForecast.py
