# *****************************************************************************
# # © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# Test and deploy the SKLearn IoTFunction
#
# Written by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe 
# *****************************************************************************
import sys,io

import logging,pprint
logger = logging.getLogger(__name__)

import utils

def main(argv):
    '''
    You can test functions locally before registering them on the server to
    understand how they work.

    Supply credentials by pasting them from the usage section into the UI.
    Place your credentials in a separate file that you don't check into the repo.
    '''

    import argparse
    parser = argparse.ArgumentParser(description=f"Tester for PredictSKLearn iotfunction")
    parser.add_argument('operation', type=str, help=f"Operation to perform. Local test, register function, store the model pickle in COS, test query on entity data, testext, regext: register extend function", choices=['test','register','store','query','testext','regext','unregext'], default='test')
    parser.add_argument('-model_file', type=str, help=f"Model file to store", default='sklearn_model.pickle')
    parser.add_argument('-model_path', type=str, help=f"Model path in COS", default='sklearn_model.pickle')
    parser.add_argument('-dependent_variables', type=str, help=f"Columns to pass to predict", default='*')
    parser.add_argument('-predicted_value', type=str, help=f"Name of the [redicted value column]", default='predicted')
    utils.add_common_args(parser,argv)
    args = parser.parse_args(argv[1:])

    # logging.basicConfig(level=args.loglevel)
    from iotfunctions.enginelog import EngineLogging
    EngineLogging.configure_console_logging(args.loglevel)

    db,db_schema=utils.setup_iotfunc(args.creds_file,args.echo_sql)
    # pprint.pprint(db.credentials)

    import phg_iotfuncs.func_sklearn
    if args.operation=='test':
        test(db,db_schema,phg_iotfuncs.func_sklearn.PredictSKLearn,
                args.model_path, args.dependent_variables,args.predicted_value)
    elif args.operation=='register':
        utils.registerFunction(db,db_schema,phg_iotfuncs.func_sklearn.PredictSKLearn)
    elif args.operation=='store':
        print(f"Storing model from {args.model_file} into COS at {args.model_path}")
        with io.open(args.model_file,'rb') as F:
            import pickle
            model_object=pickle.load(F)
        db.cos_save(model_object, args.model_path, binary=True, serialize=True)
    elif args.operation=='query':
        print(f"Query another Entity's data")
        entities=db.entity_type_metadata.keys()
        pprint.pprint(db.entity_type_metadata['test_entity_for_AMQPPreload'])
        em=db.entity_type_metadata['test_entity_for_AMQPPreload']
        df=db.read_table(em['metricTableName'],em['schemaName'], parse_dates=None, columns=None, timestamp_col=None, start_ts=None, end_ts=None, entities=None, dimension=None)
        # df=db.read_table(em['metricTableName'],em['schemaName'], parse_dates=None, columns=None, timestamp_col=em['metricTimestampColumn'], start_ts=None, end_ts=None, entities=None, dimension=em['dimensionTableName'])
        print(f"got df {df.shape}")
        print(df.head(2))
        print('.........')
        print(df.tail(2))
    elif args.operation=='testext':
        testExt(db,db_schema,phg_iotfuncs.func_sklearn.ExtendEntityPreload)
    elif args.operation=='regext':
        utils.registerFunction(db,db_schema,phg_iotfuncs.func_sklearn.ExtendEntityPreload)
    elif args.operation=='unregext':
        rc=db.unregister_functions(['ExtendEntityPreload'])
        print(f"unregistering function rc={rc}")

def test(db,db_schema,iot_func,model_path,dependent_variables,predicted_value):
    ''' Test the PredictSKLearn function

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
        fn = iot_func(model_path,dependent_variables,predicted_value)
        fn.execute_local_test(db=db,db_schema=db_schema)
    except AttributeError as attrErr:
        logger.info(f"{attrErr}")
    except Exception as exc:
        logger.exception(f"Error executing local test",exc)

def testExt(db,db_schema,iot_func,source_entity_type_name='ChemFeedInst', ts_interval=60, ts_lapse=3600, extend_entity_ok='extend_entity_ok'):
    ''' Test the ExtendEntityPreload function
    '''
    ####################################################################################

    try:
        fn = iot_func(source_entity_type_name, ts_interval, ts_lapse, extend_entity_ok)
        fn._test_entity_name='ChemCont'
        fn.execute_local_test(db=db,db_schema=db_schema,to_csv=False)
    except AttributeError as attrErr:
        logger.info(f"{attrErr}")
    except Exception as exc:
        logger.exception(f"Error executing local test",exc)

if __name__ == "__main__":
    main(sys.argv)
