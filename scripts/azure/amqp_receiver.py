# *****************************************************************************
# Adapted and fixed from https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-amqp-support
# By Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe
# Runs on Python 3.6 or 3.7
# requires https://github.com/Azure/azure-uamqp-python (pip install uamqp)
# The access key is obtained from the IoTHub Settings/Shared access policies for the predefined service
# Creating one's own SAS policy with Service connect Permission does not seem enough to get the service client authorized,
# it fails to connect to the redirection.

import sys, io, os, json
import logging
from pprint import pprint

logger = logging.getLogger(__name__)

def add_iothub_args(parser):
    parser.add_argument('--iothubcreds', type=str, help=f"IoT Hub credentials file",default=os.path.join(os.path.dirname(__file__),f"creds_iothub_{os.environ['USERNAME']}.json"))
    parser.add_argument('--iot_hub_name', type=str, help='Azure IoT Hub instance name')
    parser.add_argument('--policy_name', type=str, help='Azure IoT Hub policy name')
    parser.add_argument('--consumer_group', type=str, help='Azure IoT Hub consumer group name')
    parser.add_argument('--partition_id', type=int, help='Azure IoT Hub partition number')
    parser.add_argument('--access_key', type=str, help='Azure IoT Hub access_key')

def adjustArgs(args):
    argnames= ['iot_hub_name','policy_name','consumer_group','partition_id']
    with io.open(args.iothubcreds) as f:
        iothub_creds = json.load(f)

    # Set arguments that have not been set to their defaults from file
    v=vars(args)
    for argname in argnames:
        v[argname]=iothub_creds[argname] if v[argname] is None else v[argname]

    # return the access key
    return iothub_creds['policies_access_keys'][args.policy_name][0]

def main(argv):
    logging.basicConfig(level=logging.INFO)
    logging.getLogger(name='uamqp').setLevel(logging.WARNING)

    import argparse

    parser = argparse.ArgumentParser(description='Test AMQP Receiver')
    parser.add_argument('-since_seq', type=int, help='since message sequence number',nargs='?',default=0)
    parser.add_argument('-max_age_sec', type=int, help='maximum message age in seconds',nargs='?')
    parser.add_argument('-device_id', type=str, help='device id to filter on',nargs='?')
    parser.add_argument('-show', type=str, help='Messages to show',default='last',choices=['none','last','all'])
    parser.add_argument('-amqp_timeout', type=int, help='AMQP receive timeout in ms',default=1000)
    parser.add_argument('-df', help='Convert to DataFrame', default=False, action='store_true')
    parser.add_argument('-dateCol', dest='dateColumns',help='Name of the date columns', default=['date','Date'],nargs='*')
    # parser.add_argument('--batch_size', type=int, help=f"Batch size",default=5)

    add_iothub_args(parser)
    args = parser.parse_args(argv[1:])

    # defaults that can be overridden by command-line
    access_key=adjustArgs(args)

    logger.info(f"Starting to receive messages from {args.iot_hub_name} since_seq={args.since_seq} max_age_sec={args.max_age_sec}")

    import phg_iotfuncs.amqp_helper
    msgs=phg_iotfuncs.amqp_helper.amqpReceive(args.iot_hub_name,args.policy_name,args.consumer_group,args.partition_id,access_key,since_seq=args.since_seq,max_age_sec=args.max_age_sec,device_id=args.device_id,timeout=args.amqp_timeout)
    if args.show!='none':
        if args.df:
            import pandas as pd
            df=pd.DataFrame.from_dict(msgs)
            for dateColumn in args.dateColumns:
                if dateColumn in df.columns:
                    df[dateColumn]=pd.to_datetime(df[dateColumn])
            print(df.dtypes)
            print(df.describe())
            print(df.tail())
        else:
            for msg in msgs[-1 if args.show=='last' else 0:]:
                pprint(msg)
    print(f"Received {len(msgs)} messages, last seq={msgs[-1]['x-opt-sequence-number'] if len(msgs)>0 else 'N/A'}")

if __name__ == "__main__":
    main(sys.argv)
