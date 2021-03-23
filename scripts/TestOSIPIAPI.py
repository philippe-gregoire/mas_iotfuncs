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

def main(argv):
    ''' Test the pi points get function '''
    import os

    sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__),'..')))
    from phg_iotfuncs import osipiutils
    from phg_iotfuncs.func_osipi import POINT_ATTR_MAP,POINTS_TS_FIELD,POINTS_FIELDS,POINTS_ATTR_FIELDS,DEVICE_ATTR

    from test_OSIPiPreload import addOSIPiArgs

    import argparse
    from pprint import pprint

    # Disable warnings when using self-signed certificates
    import urllib3
    urllib3.disable_warnings()
    
    parser=argparse.ArgumentParser()
    addOSIPiArgs(argv[0],'credentials_osipi',parser)
    args=parser.parse_args(argv[1:])

    ptVals=osipiutils.getOSIPiPoints(args.pihost,args.piport,args.piuser,args.pipass,args.name_filter,POINTS_FIELDS,POINTS_ATTR_FIELDS)
    
    df=osipiutils.convertToEntity(ptVals,POINTS_TS_FIELD,args.date_field,DEVICE_ATTR,POINT_ATTR_MAP)
    print(df.head())

    max_timestamp=df[args.date_field].max()
    logger.info(f"Highest timestamp={max_timestamp} of type {type(max_timestamp)} {max_timestamp.timestamp()} {int(max_timestamp.timestamp()/1000)}")
    df.to_csv("TESTOSIPI.csv")


if __name__=='__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger.setLevel(logging.INFO)
    import sys
    main(sys.argv)
