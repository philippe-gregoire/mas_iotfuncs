# *****************************************************************************
# © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# Maximo Application Suite Analytics Service examples
#
# Test OSIPi API to get sensor Point values
#
# Author: Philippe Gregoire - IBM in France
# *****************************************************************************
import sys,logging,json,io,os
import script_utils

logger = logging.getLogger(__name__)

def testPointsAPI(args):
    ''' Test the pi Points get function '''
    from phg_iotfuncs.osipiutils import ATTR_FIELDS,getOSIPiPoints,mapPointValues,convertToEntities
    from phg_iotfuncs.func_osipi import DEVICE_ATTR

    with io.open(os.path.join(os.path.dirname(__file__),args.point_attr_map_file)) as f:
        point_attr_map=json.load(f)
    
    # Fetch the Points from OSIPi Server.
    ptVals=getOSIPiPoints(args,args.name_filter,ATTR_FIELDS)
    
    # Map values to a flattened version indexed by timestamp
    flattened=mapPointValues(ptVals,DEVICE_ATTR,point_attr_map)

    # Get into DataFrame table form indexed by timestamp 
    df=convertToEntities(flattened,args.date_field,DEVICE_ATTR)
    print(df.head())

    max_timestamp=df[args.date_field].max()
    logger.info(f"Highest timestamp={max_timestamp} of type {type(max_timestamp)} {max_timestamp.timestamp()} {int(max_timestamp.timestamp()/1000)}")
    return df

def testElementsAPI(args):
    ''' Test the pi Elements get function '''
    from phg_iotfuncs.osipiutils import ATTR_FIELDS,getOSIPiElements,convertToEntities
    from phg_iotfuncs.func_osipi import DEVICE_ATTR

     # Fetch the Elements from OSIPi Server.
    elemVals=getOSIPiElements(args,args.database_path,args.element_name,ATTR_FIELDS,DEVICE_ATTR)

    # Get into DataFrame table form indexed by timestamp 
    df=convertToEntities(elemVals,args.date_field,DEVICE_ATTR)
    print(df.head())

    max_timestamp=df[args.date_field].max()
    logger.info(f"Highest timestamp={max_timestamp} of type {type(max_timestamp)} {max_timestamp.timestamp()} {int(max_timestamp.timestamp()/1000)}")
    return df

def main(argv):
    import os
    import argparse
    from pprint import pprint
    import urllib3

    sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__),'..')))

    # Disable warnings when using self-signed certificates
    urllib3.disable_warnings()
    
    from test_OSIPiPreload import addOSIPiArgs
    parser=argparse.ArgumentParser()

    parser.add_argument('operation',help=f"Operation to perform",choices=['list','test'])
    parser.add_argument('-attributes',help='Dump attributes',action='store_true')
    parser.add_argument('-dataframe',help='Dump dataframe to file',action='store_true')
    parser.add_argument('-pathprefix',help='List Elements with this prefix only',default=None)

    addOSIPiArgs(argv[0],'credentials_osipi',parser)
    args=parser.parse_args(argv[1:])

    if args.operation=='list':
        if args.points:
            # List all Points defined in the target OSIPi server
            from phg_iotfuncs.osipiutils import listOSIPiPoints
            listOSIPiPoints(args,_log=logger.info)
        elif args.elements:
            from phg_iotfuncs.osipiutils import listOSIPiElements
            listOSIPiElements(args,dump_attributes=args.attributes,path_prefix=args.pathprefix,_log=logger.info)
        else:
            print(f"No list specified, use one of -points or -elements")
    elif args.operation=='test':
        df=None
        if args.points:
            df=testPointsAPI(args)
        elif args.elements:
            df=testElementsAPI(args)
        else:
            print(f"No test specified, use one of -points or -elements")

        if args.dataframe and df is not None:
            csvFile=f"TESTOSIPI_{'Points' if args.points else 'Elements'}.csv"
            logger.info(f"Writing dataframe to {csvFile}")
            df.to_csv(csvFile)

if __name__=='__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger.setLevel(logging.INFO)
    import sys
    main(sys.argv)
