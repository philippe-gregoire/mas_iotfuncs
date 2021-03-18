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

logger = logging.getLogger(__name__)

POINT_ATTR_MAP={
    # X Axis Sensor 1 
    'Modbus1.1.Holding16.0':('X1','globalV'),
    'Modbus1.1.Holding16.2':('X1','globalG'),
    'Modbus1.1.Holding16.4':('X1','fftv1'),
    'Modbus1.1.Holding16.6':('X1','fftg1'),
    'Modbus1.1.Holding16.8':('X1','fftv2'),
    'Modbus1.1.Holding16.10':('X1','fftg2'),
    'Modbus1.1.Holding16.12':('X1','fftv3'),
    'Modbus1.1.Holding16.14':('X1','fftg3'),
    'Modbus1.1.Holding16.16':('X1','fftv4'),
    'Modbus1.1.Holding16.18':('X1','fftg4'),
    'Modbus1.1.Holding16.20':('X1','fftv5'),
    'Modbus1.1.Holding16.22':('X1','fftg5'),
    'Modbus1.1.Holding16.24':('X1','fftv6'),
    'Modbus1.1.Holding16.26':('X1','fftg6'),
    'Modbus1.1.Holding16.28':('X1','fftv7'),
    'Modbus1.1.Holding16.30':('X1','fftg7'),
    'Modbus1.1.Holding16.32':('X1','fftv8'),
    'Modbus1.1.Holding16.34':('X1','fftg8'),
    'Modbus1.1.Holding16.36':('X1','fftv7_2'),
    'Modbus1.1.Holding16.38':('X1','fftg7_2'),
    'Modbus1.1.Holding16.40':('X1','fftv5_7'),
    'Modbus1.1.Holding16.42':('X1','fftg5_7'),
    'Modbus1.1.Holding16.44':('X1','temp'),
    # Y Axis Sensor 1(
    'Modbus1.1.Holding16.46':('Y1','globalV'),
    'Modbus1.1.Holding16.48':('Y1','globalG'),
    'Modbus1.1.Holding16.50':('Y1','fftv1'),
    'Modbus1.1.Holding16.52':('Y1','fftg1'),
    'Modbus1.1.Holding16.54':('Y1','fftv2'),
    'Modbus1.1.Holding16.56':('Y1','fftg2'),
    'Modbus1.1.Holding16.58':('Y1','fftv3'),
    'Modbus1.1.Holding16.60':('Y1','fftg3'),
    'Modbus1.1.Holding16.62':('Y1','fftv4'),
    'Modbus1.1.Holding16.64':('Y1','fftg4'),
    'Modbus1.1.Holding16.66':('Y1','fftv5'),
    'Modbus1.1.Holding16.68':('Y1','fftg5'),
    'Modbus1.1.Holding16.70':('Y1','fftv6'),
    'Modbus1.1.Holding16.72':('Y1','fftg6'),
    'Modbus1.1.Holding16.74':('Y1','fftv7'),
    'Modbus1.1.Holding16.76':('Y1','fftg7'),
    'Modbus1.1.Holding16.78':('Y1','fftv8'),
    'Modbus1.1.Holding16.80':('Y1','fftg8'),
    'Modbus1.1.Holding16.82':('Y1','fftv7_2'),
    'Modbus1.1.Holding16.84':('Y1','fftg7_2'),
    'Modbus1.1.Holding16.86':('Y1','fftv5_7'),
    'Modbus1.1.Holding16.88':('Y1','fftg5_7'),
    'Modbus1.1.Holding16.90':('Y1','temp'),
    # X Axis sensor 2)
    'Modbus1.1.Holdin)g16.92':('X2','globalV'),
    'Modbus1.1.Holdin)g16.94':('X2','globalG'),
    'Modbus1.1.Holdin)g16.96':('X2','fftv1'),
    'Modbus1.1.Holding16.98':('X2','fftg1'),
    'Modbus1.1.Holding16.100':('X2','fftv2'),
    'Modbus1.1.Holding16.102':('X2','fftg2'),
    'Modbus1.1.Holding16.104':('X2','fftv3'),
    'Modbus1.1.Holding16.106':('X2','fftg3'),
    'Modbus1.1.Holding16.108':('X2','fftv4'),
    'Modbus1.1.Holding16.110':('X2','fftg4'),
    'Modbus1.1.Holding16.112':('X2','fftv5'),
    'Modbus1.1.Holding16.114':('X2','fftg5'),
    'Modbus1.1.Holding16.116':('X2','fftv6'),
    'Modbus1.1.Holding16.118':('X2','fftg6'),
    'Modbus1.1.Holding16.120':('X2','fftv7'),
    'Modbus1.1.Holding16.122':('X2','fftg7'),
    'Modbus1.1.Holding16.124':('X2','fftv8'),
    'Modbus1.1.Holding16.126':('X2','fftg8'),
    'Modbus1.1.Holding16.128':('X2','fftv7_2'),
    'Modbus1.1.Holding16.130':('X2','fftg7_2'),
    'Modbus1.1.Holding16.132':('X2','fftv5_7'),
    'Modbus1.1.Holding16.134':('X2','fftg5_7'),
    'Modbus1.1.Holding16.136':('X2','temp'),
    'Modbus1.1.Holding16.138':(None,'rpm')
}

DEVICE_ATTR='deviceid'

# List the fields we want to retrieve for Points
# POINTS_FIELDS=['Name','Descriptor','EngineeringUnits']
POINTS_FIELDS=['Name'] # other fields are empty
# List the values attributes we want to retrieve
VALUE_FIELDS=['Value','Timestamp']

def main(argv):
    ''' Test the pi points get function '''
    import os
    sys.path.append(os.path.realpath(os.path.join(os.path.dirname(__file__),'..')))
    from phg_iotfuncs import osipiutils

    import argparse
    from pprint import pprint

    # Disable warnings when using self-signed certificates
    import urllib3
    urllib3.disable_warnings()
    
    import script_utils

    parser=argparse.ArgumentParser()
    creds_pi=script_utils.load_creds_file(argv[0],'credentials_osipi')
    for arg in ['pihost','piport','picert','piuser','pipass','nameFilter']:
        parser.add_argument('-'+arg,required=False,default=creds_pi[arg] if arg in creds_pi else None)
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
