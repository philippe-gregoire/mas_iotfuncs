import sys,os
import datetime as dt
import json
import pandas as pd
import numpy as np
import logging
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from iotfunctions.db import Database
from iotfunctions.enginelog import EngineLogging

EngineLogging.configure_console_logging(logging.DEBUG)

'''
You can test functions locally before registering them on the server to
understand how they work.

Supply credentials by pasting them from the usage section into the UI.
Place your credentials in a separate file that you don't check into the repo.

'''
credPath=os.path.join(os.path.dirname(os.path.abspath(sys.argv[0])),'credentials_as.json')
print(f"Loading Analytics Service credentials from {credPath}")
with open(credPath, encoding='utf-8') as F:
    credentials = json.loads(F.read())
db_schema = None
if 'postgresql' in credentials:
    credentials['postgresql']['databaseName']=credentials['postgresql']['db']
db = Database(credentials=credentials)

'''
Import and instantiate the functions to be tested

The local test will generate data instead of using server data.
By default it will assume that the input data items are numeric.

Required data items will be inferred from the function inputs.

The function below executes an expression involving a column called x1
The local test function will generate data dataframe containing the column x1

By default test results are written to a file named df_test_entity_for_<function_name>
This file will be written to the working directory.

'''

import phg_iotfuncs.functions
# import HelloWorldPhG, CSVDataSource, CSVPreload, HTTPPreload, MergeSampleTimeSeries

####################################################################################
# fn = HelloWorldPhG(
#         name = 'AS_Tester',
#         greeting_col = 'greeting')
# fn.execute_local_test(db=db,db_schema=db_schema)
# db.register_functions([HelloWorldPhG])

####################################################################################
# fncsv = phg_iotfuncs.functions.CSVDataSource(
#         # csv_file = 'extract_CaCO3_cont1.csv')
#         input_items=['extract_CaCO3_cont1.csv'],output_items=['test'])
# fncsv.execute_local_test(db=db,db_schema=db_schema)
# db.register_functions([phg_iotfuncs.functions.CSVDataSource])

####################################################################################
fncsv = phg_iotfuncs.functions.CSVPreload(
        # csv_file = 'extract_CaCO3_cont1.csv',
        csv_file = '*.csv',
        rebaseTS = True,
        output_item='loaded')

try:
    fncsv.execute_local_test(db=db,db_schema=db_schema)
except Exception as exc:
    print(f"Exception {exc}")
    import traceback
    traceback.print_exc()
    pass
db.register_functions([phg_iotfuncs.functions.CSVPreload])


####################################################################################
# fnpre = phg_iotfuncs.functions.HTTPPreload(url='internal_test',request='GET',output_item='loaded')
# fnpre.execute_local_test(db=db,db_schema=db_schema)
# db.register_functions([phg_iotfuncs.functions.HTTPPreload])

####################################################################################
# fnts=phg_iotfuncs.functions.MergeSampleTimeSeries(input_items=['temp', 'pressure', 'velocity'],
#                             output_items=['temp', 'pressure', 'velocity'])
# db.register_functions([phg_iotfuncs.functions.MergeSampleTimeSeries])

# db.register_functions([HelloWorldPhG,CSVDataSource,HTTPPreload,MergeSampleTimeSeries])
