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
import os,sys

import logging,pprint
from iotfunctions.enginelog import EngineLogging
logger = logging.getLogger(__name__)

import script_utils

## Hardcoded values, will parametrize later
# Map of points to attributes
POINT_PREFIX='Modbus1.1.Holding16.'
POINT_ATTR_MAP_FILE="POINT_ATTR_MAP.json"

def addOSIPiArgs(refPath,credsFile,parser):

    creds_pi=script_utils.load_creds_file(refPath,credsFile)
    for arg in ['pihost','piport','piuser','pipass']:
        parser.add_argument('-'+arg,required=False,default=creds_pi[arg] if arg in creds_pi else None)

    parser.add_argument('-date_field', type=str, help=f"Field containing the event date/timestamp", required=False,default='date')
    parser.add_argument('-point_attr_map_file', type=str, help=f"OSIPi Points mapping JSON file name", required=False,default=POINT_ATTR_MAP_FILE)
    parser.add_argument('-name_filter', type=str, help=f"OSIPi Point name filter", required=False,default=f"{POINT_PREFIX}*")

    parser.add_argument('-databasePath', type=str, help=f"Path to the database, e.g \\\\OSISOFT-SERVER\\IBM_FabLab", required=False,default='\\\\OSISOFT-SERVER\\IBM_FabLab')
    parser.add_argument('-elementName', type=str, help=f"Parent Element name", required=False,default='Motor')

def main(argv):
    '''
    You can test functions locally before registering them on the server to
    understand how they work.

    Supply credentials by pasting them from the usage section into the UI.
    Place your credentials in a separate file that you don't check into the repo.
    '''
    # Get the IoTFunctions lib path
    import os, sys, argparse
    sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__),'..')))

    parser = argparse.ArgumentParser(description=f"Tester for OSIPI iotfunctions")
    addOSIPiArgs(argv[0],'credentials_osipi',parser)

    parser.add_argument('operation', type=str, help=f"Operation", choices=['test','register','dbtest','create','constant','k','list'], default='test')
    parser.add_argument('ositype', type=str, help=f"Type of OSI API", choices=['Points','Elements'])
    parser.add_argument('-const_name', type=str, help=f"Name of constant", default=None)
    parser.add_argument('-const_value', type=str, help=f"Value for constant", default=None)
    parser.add_argument('-entityNamePrefix', type=str, help=f"Value for constant", default=f"test_entity_for_")
    parser.add_argument('-database_path', type=str, help=f"Value for constant", default=None)
    parser.add_argument('-element_name', type=str, help=f"Value for constant", default=None)

    script_utils.add_common_args(parser,argv)
    args = parser.parse_args(argv[1:])

    # logging.basicConfig(level=args.loglevel)
    EngineLogging.configure_console_logging(args.loglevel)

    db,db_schema=script_utils.setup_iotfunc(args.creds_file,args.echo_sql)
    # pprint.pprint(db.credentials)
    import os

    if args.ositype=='Points':
        from phg_iotfuncs.func_osipi import PhGOSIPIPointsPreload as TargetFunc
        entityName=args.entityNamePrefix+TargetFunc.__name__
        if args.operation=='test':
            test(db,db_schema,TargetFunc,
                    args.pihost, args.piport,
                    args.piuser,args.pipass,
                    args.name_filter,args.date_field)
        elif args.operation=='register':
            script_utils.registerFunction(db,db_schema,TargetFunc)
        elif args.operation=='create':
            from phg_iotfuncs.func_osipi import POINT_ATTR_MAP
            attributes=list(dict.fromkeys([v[1] for v in POINT_ATTR_MAP.values()]))
            print(f"Creating entity {entityName} with attributes {attributes}")
            script_utils.createEntity(db,db_schema,entityName,attributes)
        elif args.operation=='list':
            # List all Points defined in the target OSIPi server
            from phg_iotfuncs.osipiutils import listOSIPiPoints
            listOSIPiPoints(args)

    elif args.ositype=='Elements':
        from phg_iotfuncs.func_osipi import PhGOSIElemsPreload as TargetFunc
        entityName=args.entityNamePrefix+TargetFunc.__name__
        if args.operation=='test':
            test(db,db_schema,TargetFunc,
                    args.pihost, args.piport,
                    args.piuser,args.pipass,
                    args.database_path,args.element_name,
                    args.date_field)
        elif args.operation=='register':
            script_utils.registerFunction(db,db_schema,TargetFunc)
        elif args.operation=='create':
            # get a data sample to figure out the attributes
            from phg_iotfuncs.osipiutils import ATTR_FIELDS,getOSIPiElements,convertToEntities
            from phg_iotfuncs.func_osipi import DEVICE_ATTR

            # Fetch the Elements from OSIPi Server.
            elemVals=getOSIPiElements(args,args.database_path,args.element_name,ATTR_FIELDS,DEVICE_ATTR)

            # Get into DataFrame table form indexed by timestamp 
            df=convertToEntities(elemVals,args.date_field,DEVICE_ATTR)
            attributes=df.columns
            print(f"Creating entity {entityName} with attributes {attributes}")
            script_utils.createEntity(db,db_schema,entityName,attributes)
        elif args.operation=='dbtest':
            import iotfunctions
            from phg_iotfuncs import iotf_utils

            # get a data sample to figure out the attributes
            from phg_iotfuncs.osipiutils import ATTR_FIELDS,getOSIPiElements,convertToEntities
            from phg_iotfuncs.func_osipi import DEVICE_ATTR

            # Fetch the Elements from OSIPi Server.
            elemVals=getOSIPiElements(args,args.database_path,args.element_name,ATTR_FIELDS,DEVICE_ATTR)

            # Get into DataFrame table form indexed by timestamp 
            df=convertToEntities(elemVals,args.date_field,DEVICE_ATTR)
            
            entity_type_dict,entity_meta_dict=iotfunctions.metadata.retrieve_entity_type_metadata(_db=db,logical_name=entityName)
            iotf_utils.renameToDBColumns(df,entity_meta_dict)

            iotf_utils.adjustDataFrameColumns(db,entity_meta_dict,df,'OSItest',['date'])

            rc=db.write_frame(df=df, table_name=entity_meta_dict['metricsTableName'])
            print(f"Written {len(df)} rows, rc={rc}")

    elif args.operation=='constant':
        from phg_iotfuncs import iotf_utils
        pprint.pprint(iotf_utils.getConstant(db, constant_name=None))
        if args.const_name is not None and args.const_value is not None:
            iotf_utils.putConstant(db,args.const_name,args.const_value)
    elif args.operation=='list_k':
        from phg_iotfuncs import iotf_utils
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

def test(db,db_schema,iot_func,osipi_host, osipi_port,
            osipi_user, osipi_pass, 
            name_filter, date_field):
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
        logger.info(f"Creating {iot_func} for {osipi_host}:{osipi_port}")
        fn = iot_func(osipi_host, osipi_port, osipi_user, osipi_pass, 
                    name_filter, date_field,
                    osipi_preload_ok='osipi_preload_ok')
        logger.info(f"Executing test for {iot_func}")
        fn.execute_local_test(db=db,db_schema=db_schema,to_csv=False)
    except AttributeError as attrErr:
        logger.info(f"{attrErr}")
    except Exception as exc:
        logger.exception(f"Error executing local test",exc)

if __name__ == "__main__":
    main(sys.argv)
