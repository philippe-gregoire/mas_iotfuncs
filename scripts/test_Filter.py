# *****************************************************************************
# # © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# Test and deploy the Filter IoTFunction
#
# Written by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe 
# *****************************************************************************
import sys,io,os

import logging,pprint
logger = logging.getLogger(__name__)

import script_utils

def main(argv):
    '''
    You can test functions locally before registering them on the server to
    understand how they work.

    Supply credentials by pasting them from the usage section into the UI.
    Place your credentials in a separate file that you don't check into the repo.
    '''
    sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__),'..')))

    import argparse
    parser = argparse.ArgumentParser(description=f"Tester for PredictSKLearn iotfunction")
    parser.add_argument('operation', type=str, help=f"Operation to perform. Local test, register function, store the filter function", choices=['test','register'], default='test')
    parser.add_argument('-timestamp_col', type=str, help=f"Model file to store", default='')
    parser.add_argument('-keep_timestamp', type=str, help=f"Model path in COS", default='')
    script_utils.add_common_args(parser,argv)
    args = parser.parse_args(argv[1:])

    # logging.basicConfig(level=args.loglevel)
    from iotfunctions.enginelog import EngineLogging
    EngineLogging.configure_console_logging(args.loglevel)

    db,db_schema=script_utils.setup_iotfunc(args.creds_file,args.echo_sql)
    # pprint.pprint(db.credentials)

    import phg_iotfuncs.func_base
    iotFunc=phg_iotfuncs.func_base.PhGFilterMultiplicates
    if args.operation=='test':
        test(db,db_schema,iotFunc,args.timestamp_col, args.keep_timestamp)
    elif args.operation=='register':
        script_utils.registerFunction(db,db_schema,iotFunc)
    elif args.operation=='unregfunc':
        rc=db.unregister_functions(['PhGFilterMultiplicates'])
        print(f"unregistering function rc={rc}")
    else:
        print(f"Unknown operation {args.operation}")

def test(db,db_schema,iot_func,timestamp_col,depenkeep_timestamp):
    ''' Test the PhGFilterMultiplicates function

    Import and instantiate the functions to be tested

    The local test will generate data instead of using server data.
    By default it will assume that the input data items are numeric.

    Required data items will be inferred from the function inputs.

    The function below executes an expression involving a column called x1
    The local test function will generate data dataframe containing the column x1

    By default test results are written to a file named df_test_entity_for_<function_name>
    This file will be written to the working directory.
    '''
    ####################################################################################

    try:
        fn = iot_func(timestamp_col,depenkeep_timestamp)
        fn.execute_local_test(db=db,db_schema=db_schema)
    except AttributeError as attrErr:
        logger.info(f"{attrErr}")
    except Exception as exc:
        logger.exception(f"Error executing local test",exc)

if __name__ == "__main__":
    main(sys.argv)
