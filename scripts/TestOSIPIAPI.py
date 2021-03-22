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

DEVICE_ATTR='deviceid'

# List the fields we want to retrieve for Points
# POINTS_FIELDS=['Name','Descriptor','EngineeringUnits']
POINTS_FIELDS=['Name'] # other fields are empty
# List the values attributes we want to retrieve
VALUE_FIELDS=['Value','Timestamp']

def addOSIPiArgs(refPath,credsFile,parser):
    creds_pi=script_utils.load_creds_file(refPath,credsFile)
    for arg in ['pihost','piport','picert','piuser','pipass','nameFilter']:
        parser.add_argument('-'+arg,required=False,default=creds_pi[arg] if arg in creds_pi else None)

def main(argv):
    ''' Test the pi points get function '''
    import os
    sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__),'..')))
    from phg_iotfuncs import osipiutils
    from phg_iotfuncs.func_osipi import POINT_PREFIX, POINT_ATTR_MAP

    import argparse
    from pprint import pprint

    # Disable warnings when using self-signed certificates
    import urllib3
    urllib3.disable_warnings()
    

    parser=argparse.ArgumentParser()
    addOSIPiArgs(argv[0],'credentials_osipi',parser)
    args=parser.parse_args(argv[1:])

    ptVals=osipiutils.getOSIPiPoints(args.pihost,args.piport,args.piuser,args.pipass,args.nameFilter,POINTS_FIELDS,VALUE_FIELDS)
    # pprint(ptVals)

    mapped=osipiutils.mapValues(ptVals,DEVICE_ATTR,POINT_ATTR_MAP)
    pprint(mapped.values())

    import pandas as pd
    df=pd.DataFrame.from_records([v for v in mapped.values()])
    logger.info(f"df initial columns={[c for c in df.columns]}")
    print(df.head())

if __name__=='__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger.setLevel(logging.INFO)
    import sys
    main(sys.argv)
