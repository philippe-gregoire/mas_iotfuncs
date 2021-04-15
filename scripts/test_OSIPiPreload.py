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
    parser.add_argument('-points', help=f"Use OSI Points API", required=False, action='store_true')
    parser.add_argument('-elements', help=f"use OSIPi Elements API", required=False, action='store_true')

    for arg in ['pihost','piport','piuser','pipass']:
        parser.add_argument('-'+arg,required=False,default=creds_pi[arg] if arg in creds_pi else None)

    parser.add_argument('-entity_type', type=str, help=f"Entity type name", required=False,default=None)
    parser.add_argument('-date_field', type=str, help=f"Field containing the event date/timestamp", required=False,default='date')

    parser.add_argument('-point_attr_map_file', type=str, help=f"OSIPi Points mapping JSON file name", required=False,default=None)
    parser.add_argument('-points_name_prefix', type=str, help=f"OSIPi Points name prefix", required=False,default=None)

    parser.add_argument('-parent_element_path', type=str, help=f"OSIPi Parent Element name", required=False,default=None)

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

    script_utils.add_operations(parser,['osi_dbtest','osi_list'])
    script_utils.add_common_args(parser,argv)

    addOSIPiArgs(argv[0],'credentials_osipi',parser)

    args = parser.parse_args(argv[1:])

    # logging.basicConfig(level=args.loglevel)
    EngineLogging.configure_console_logging(args.loglevel)

    db,db_schema=script_utils.setup_iotfunc(args.creds_file,args.echo_sql)
    # pprint.pprint(db.credentials)
    import os

    if args.points:
        from phg_iotfuncs.func_osipi import PhGOSIPIPointsPreload as TargetFunc
        entityName=args.entity_type if args.entity_type else args.entityNamePrefix+TargetFunc.__name__
        if args.operation=='test':
            point_attr_map=script_utils.loadPointsAttrMap(args.point_attr_map_file)
            test(db,db_schema,
                    TargetFunc(args.pihost, args.piport,args.piuser,args.pipass,
                                args.points_name_prefix, point_attr_map, args.date_field,
                                'osipi_preload_ok'))
        elif args.operation=='register':
            script_utils.registerFunction(db,db_schema,TargetFunc)
        elif args.operation=='create':
            if not args.point_attr_map_file or not args.points_name_prefix:
                print(f"-point_attr_map_file and -points_name_prefix must be specified for operation {args.operation}")
                return

            point_attr_map=script_utils.loadPointsAttrMap(args.point_attr_map_file)

            # Create the list of columns
            columns=[script_utils.to_sqlalchemy_column(v[1],v[2] if len(v)>2 else float,args.date_field) for v in point_attr_map.values()]                

            # attributes=list(dict.fromkeys([v[1] for v in point_attr_map.values()]))

            print(f"Creating entity {entityName} with columns {columns} specified in {args.point_attr_map_file}")
            script_utils.createEntity(db,db_schema,entityName,columns,
                    function=TargetFunc,
                    func_input={
                        'osipi_host': args.pihost,
                        'osipi_port': args.piport,
                        'osipi_user': args.piuser,
                        'osipi_pass': args.pipass, 
                        'date_field': args.date_field,
                        'name_filter': args.points_name_prefix,
                        'points_attr_map': point_attr_map
                    },
                    func_output={'osipi_preload_ok':'osipi_preload_ok'})
            # script_utils.createEntity(db,db_schema,entityName,attributes)
        elif args.operation=='osi_list':
            # List all Points defined in the target OSIPi server
            from phg_iotfuncs.osipiutils import listOSIPiPoints
            listOSIPiPoints(args)

    elif args.elements:
        from phg_iotfuncs.func_osipi import PhGOSIElemsPreload as TargetFunc
        entityName=args.entity_type if args.entity_type else args.entityNamePrefix+TargetFunc.__name__
        if args.operation=='test':
            test(db,db_schema,
                    TargetFunc(
                        args.pihost, args.piport,args.piuser,args.pipass,
                        args.parent_element_path,args.date_field,
                        'osipi_elements_preload_ok'))
        elif args.operation=='register':
            script_utils.registerFunction(db,db_schema,TargetFunc)
        elif args.operation=='create':
            # get a data sample to figure out the attributes
            from phg_iotfuncs.osipiutils import ATTR_FIELDS,getOSIPiElements,convertToEntities
            from phg_iotfuncs.func_osipi import DEVICE_ATTR

            if not args.parent_element_path:
                print(f"-parent_element_path must be specified for operation {args.operation}")
                return

            # Fetch the Elements from OSIPi Server.
            elemVals=getOSIPiElements(args,args.parent_element_path,ATTR_FIELDS,DEVICE_ATTR)

            # Get into DataFrame table form indexed by timestamp 
            df=convertToEntities(elemVals,args.date_field,DEVICE_ATTR)
            attributes=df.columns
            print(f"Creating entity {entityName} with attributes {attributes}")
            script_utils.createEntity(db,db_schema,entityName,attributes,
                    function=TargetFunc,
                    func_input={
                        'osipi_host': args.pihost,
                        'osipi_port': args.piport,
                        'osipi_user': args.piuser,
                        'osipi_pass': args.pipass, 
                        'date_field': args.date_field,
                        'parent_element_path': args.parent_element_path
                    },
                    func_output={'osipi_elements_preload_ok':'osipi_elements_preload_ok'})

            #script_utils.createEntity(db,db_schema,entityName,attributes)
        elif args.operation=='osi_list':
            # List all Elements defined in the target OSIPi server
            from phg_iotfuncs.osipiutils import listOSIPiElements
            listOSIPiElements(args)
        elif args.operation=='osi_dbtest':
            import iotfunctions
            from phg_iotfuncs import iotf_utils

            # get a data sample to figure out the attributes
            from phg_iotfuncs.osipiutils import ATTR_FIELDS,getOSIPiElements,convertToEntities
            from phg_iotfuncs.func_osipi import DEVICE_ATTR

            # Fetch the Elements from OSIPi Server.
            elemVals=getOSIPiElements(args,args.parent_element_path,ATTR_FIELDS,DEVICE_ATTR)

            # Get into DataFrame table form indexed by timestamp 
            df=convertToEntities(elemVals,args.date_field,DEVICE_ATTR)
            
            entity_type_dict,entity_meta_dict=iotfunctions.metadata.retrieve_entity_type_metadata(_db=db,logical_name=entityName)
            iotf_utils.renameToDBColumns(df,entity_meta_dict)

            iotf_utils.adjustDataFrameColumns(db,entity_meta_dict,df,'OSItest',['date'])

            rc=db.write_frame(df=df, table_name=entity_meta_dict['metricsTableName'])
            print(f"Written {len(df)} rows, rc={rc}")
    else:
        script_utils.common_operation(args,db,db_schema)

def test(db,db_schema,iot_func):
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
        logger.info(f"Executing test for {iot_func.__name__}")
        iot_func.execute_local_test(db=db,db_schema=db_schema,to_csv=False)
    except AttributeError as attrErr:
        logger.info(f"{attrErr}")
    except Exception as exc:
        logger.exception(f"Error executing local test",exc)

if __name__ == "__main__":
    main(sys.argv)
