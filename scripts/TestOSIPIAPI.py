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
import sys,logging
import script_utils

logger = logging.getLogger(__name__)

def testPointsAPI(args):
    ''' Test the pi Points get function '''
    from phg_iotfuncs.osipiutils import ATTR_FIELDS,getOSIPiPoints,mapPointValues,convertToEntities
    from phg_iotfuncs.func_osipi import DEVICE_ATTR,POINT_ATTR_MAP
    
    # Fetch the Points from OSIPi Server.
    ptVals=getOSIPiPoints(args,args.name_filter,ATTR_FIELDS)
    
    # Map values to a flattened version indexed by timestamp
    flattened=mapPointValues(ptVals,DEVICE_ATTR,POINT_ATTR_MAP)

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
    elemVals=getOSIPiElements(args,args.databasePath,args.elementName,ATTR_FIELDS,DEVICE_ATTR)

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
    parser.add_argument('-test', type=str, help=f"test to run, either Elements or Points", required=False,default='Elements')
    parser.add_argument('-databasePath', type=str, help=f"Path to the database, e.g \\\\OSISOFT-SERVER\\IBM_FabLab", required=False,default='\\\\OSISOFT-SERVER\\IBM_FabLab')
    parser.add_argument('-elementName', type=str, help=f"Parent Element name", required=False,default='Motor')

    addOSIPiArgs(argv[0],'credentials_osipi',parser)
    args=parser.parse_args(argv[1:])

    df=None
    if args.test=='Points':
        df=testPointsAPI(args)
    elif args.test=='Elements':
        df=testElementsAPI(args)

    if df is not None:
        df.to_csv("TESTOSIPI_Points.csv")

if __name__=='__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger.setLevel(logging.INFO)
    import sys
    main(sys.argv)
