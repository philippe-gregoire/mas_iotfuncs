# *****************************************************************************
# © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# Test and deploy the OSIPiPreload IoTFunction
#
# Written by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe 
# *****************************************************************************
import sys

import logging,pprint
from iotfunctions.enginelog import EngineLogging
logger = logging.getLogger(__name__)

import script_utils

def main(argv):
    '''
    You can test functions locally before registering them on the server to
    understand how they work.

    Supply credentials by pasting them from the usage section into the UI.
    Place your credentials in a separate file that you don't check into the repo.
    '''

    import argparse
    parser = argparse.ArgumentParser(description=f"Tester for OSIPIPreload iotfunction")
    parser.add_argument('operation', type=str, help=f"Operation", choices=['test','register','constant','k'], default='test')
    parser.add_argument('-date', dest='date_field', type=str, help=f"Field containing the event date/tiestamp", default='date')
    parser.add_argument('-device_id', type=str, help=f"Device ID to filter on")
    parser.add_argument('-req', dest='required_fields', type=str, help=f"Fields that are required to retain the record", nargs='*')
    parser.add_argument('-const_name', type=str, help=f"Name of constant", default=None)
    parser.add_argument('-const_value', type=int, help=f"Value for constant", default=None)

    script_utils.add_common_args(parser,argv)
    args = parser.parse_args(argv[1:])

    # logging.basicConfig(level=args.loglevel)
    EngineLogging.configure_console_logging(args.loglevel)

    db,db_schema=script_utils.setup_iotfunc(args.creds_file,args.echo_sql)
    # pprint.pprint(db.credentials)
    import os
    sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__),'..')))
    
    from phg_iotfuncs.func_osipi import PhGOSIPIPreload as TargetFunc
    if args.operation=='test':
        access_key=amqp_receiver.adjustArgs(args)
        required_fields='' if args.required_fields is None else ','.join(args.required_fields)
        test(db,db_schema,TargetFunc,
                args.iot_hub_name, args.policy_name,
                args.consumer_group,args.partition_id,access_key,
                args.device_id,args.date_field,required_fields)
    elif args.operation=='register':
        script_utils.registerFunction(db,db_schema,TargetFunc)
    elif args.operation=='constant':
        from phg_iotfuncs import iotf_utils
        pprint.pprint(iotf_utils.getConstant(db, constant_name=None))
        if args.const_name is not None and args.const_value is not None:
            iotf_utils.putConstant(db,args.const_name,args.const_value)
    elif args.operation=='k':
        from s import iotf_utils
        k_name=args.lastseq_constant
        k_desc='PhG Konst'
        try:
            rc=iotf_utils.registerConstant(db,k_name,int,k_desc)
        except:
            pass
        k_val=iotf_utils.getConstant(db,k_name,-1)
        print(f"Got value {k_val}")
        rc=iotf_utils.putConstant(db,k_name,k_val+1)
        k_newval=iotf_utils.getConstant(db,k_name)
        print(f"Got new value {k_newval}")

    elif args.operation=='k':
        import iotfunctions.ui
        constants = [iotfunctions.ui.UISingle(name='phg_const',description='PhG Konst',datatype=int)]
        payload = []
        for c in constants:
            meta = c.to_metadata()
            name = meta['name']
            default = meta.get('value', None)
            del meta['name']
            try:
                del meta['value']
            except KeyError:
                pass
            payload.append({'name': name, 'entityType': None, 'enabled': True, 'value': default, 'metadata': meta})
        pprint.pprint(payload)
        rc=db.http_request(object_type='defaultConstants', object_name=None, request="POST", payload=payload,
                          raise_error=True)
        pprint(rc)

def test(db,db_schema,iot_func,iot_hub_name, policy_name, consumer_group, partition_id, access_key, device_id, date_field, required_fields):
    ''' Test the PhGOSIPIPreload function

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
        logger.info(f"Creating {iot_func} for {iot_hub_name}, {policy_name}, {consumer_group}, {partition_id}, {access_key}, {device_id},{date_field},{required_fields}")
        fn = iot_func(iot_hub_name, policy_name, consumer_group, partition_id, access_key,
                      device_id,date_field,required_fields,
                      amqp_preload_ok='amqp_preload_ok')
        logger.info(f"Executing test for {iot_func}")
        fn.execute_local_test(db=db,db_schema=db_schema,to_csv=False)
    except AttributeError as attrErr:
        logger.info(f"{attrErr}")
    except Exception as exc:
        logger.exception(f"Error executing local test",exc)

if __name__ == "__main__":
    main(sys.argv)
