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
    from phg_iotfuncs import func_osipi,osipiutils

    point_attr_map=script_utils.loadJSON(args.point_attr_map_file)
    # with io.open(os.path.join(os.path.dirname(__file__),args.point_attr_map_file)) as f:
    #     point_attr_map=json.load(f)
    
    # Fetch the Points from OSIPi Server.
    attrFields=[osipiutils.ATTR_FIELD_VAL,osipiutils.ATTR_FIELD_TS]
    ptVals=osipiutils.getOSIPiPoints(args,args.points_name_prefix,attrFields)
    
    # Map values to a flattened version indexed by timestamp
    flattened=osipiutils.mapPointValues(ptVals,func_osipi.DEVICE_ATTR,point_attr_map)

    # Get into DataFrame table form indexed by timestamp 
    df=osipiutils.convertToEntities(flattened,args.date_field,func_osipi.DEVICE_ATTR)
    #print(df.head())

    max_timestamp=df[args.date_field].max()
    logger.info(f"Highest timestamp={max_timestamp} of type {type(max_timestamp)} {max_timestamp.timestamp()} {int(max_timestamp.timestamp()/1000)}")
    return df

def testElementsAPI(args):
    ''' Test the pi Elements get function '''
    from  phg_iotfuncs import osipiutils,func_osipi

    # Fetch the Elements from OSIPi Server.
    attrFields=[osipiutils.ATTR_FIELD_VAL,osipiutils.ATTR_FIELD_TS]
    elemVals,rawElemsJSON=osipiutils.getOSIPiElements(args,args.parent_element_path,attrFields,func_osipi.DEVICE_ATTR,startTime=args.startTime,interval=args.interval)

    # Get into DataFrame table form indexed by timestamp 
    df=osipiutils.convertToEntities(elemVals,args.date_field,func_osipi.DEVICE_ATTR)
    max_timestamp=df[args.date_field].max()
    logger.info(f"Read {len(df)} rows, timestamp from {df[args.date_field].min().isoformat()} to {df[args.date_field].max().isoformat()} ")

    return df,rawElemsJSON

def main(argv):
    import os,io
    import argparse
    from pprint import pprint,pformat
    import urllib3

    sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__),'..')))

    # Disable warnings when using self-signed certificates
    urllib3.disable_warnings()
    
    import test_OSIPiPreload
    parser=argparse.ArgumentParser()

    parser.add_argument('operation',help=f"Operation to perform",choices=['list','test'])
    parser.add_argument('-attributes',help='Dump attributes',action='store_true')
    parser.add_argument('-to_csv',help='Dump dataframe to CSV file',action='store_true')
    parser.add_argument('-to_json',help='Dump raw data JSON file',action='store_true')
    parser.add_argument('-pathprefix',help='List Elements with this prefix only',default=None)
    parser.add_argument('-startTime',help='Start time',default=None)
    parser.add_argument('-interval',help='Interpolation interval (1s, 10s, 1h, 1m)',default=None)

    test_OSIPiPreload.addOSIPiArgs(argv[0],'credentials_osipi',parser)
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
            df,rawElemsJSON=testElementsAPI(args)
        else:
            print(f"No test specified, use one of -points or -elements")

        max_ts=df[args.date_field].max().strftime('%Y-%m-%d_%H-%M-%S_%f')
        outFile=os.path.abspath(f"TESTOSIPI_{'Points' if args.points else 'Elements'}_{max_ts}")
        if args.to_csv and df is not None:
            logger.info(f"Writing dataframe to {outFile}.csv")
            df.to_csv(f"{outFile}.csv")
        if args.to_json and rawElemsJSON is not None:
            logger.info(f"Writing ram JSON to {outFile}.json")
            with io.open(f"{outFile}.json",'w') as f:
                f.write(pformat(rawElemsJSON))

if __name__=='__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger.setLevel(logging.INFO)
    import sys
    main(sys.argv)