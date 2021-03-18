## Act as a device to send data works on Python 3.7 (but not 3.6, async.run() not there)
# Send data from a CSV file as an IoT Device
# Requires 'pip install azure-iot-device'
# Adapted by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe


# You will get the connection string from your device on IoTHub
import sys, os, io, asyncio, json

import azure.iot.device.aio
import azure.iot.device

async def main(argv):

    import argparse
    parser = argparse.ArgumentParser(description=f"Tester for AMQPPreload iotfunction")
    parser.add_argument('deviceId', type=str, help=f"Device ID (or csv file if omitted, in which case deviceId is the last part of filename after -@)")
    parser.add_argument('csvFile', type=str, help=f"Name of CSV file", nargs='?',default=None)
    parser.add_argument('-count', type=int, help=f"Count of messages to send", default=10)
    parser.add_argument('-offset', type=int, help=f"Offset of first message to send", default='0')
    parser.add_argument('-norebase', help=f"Do not rebase time stamp from now", action='store_true')
    parser.add_argument('-pace', help=f"Pace data sending, respecting timestamps intervals", action='store_true')
    parser.add_argument('-maxDelayMult', type=float, help=f"When pacing, clip time gaps that are over x times the previous value", default=1.5)
    parser.add_argument('-dateField', type=str, help=f"Date field, default to first column", default=None)
    parser.add_argument('-test', help=f"Test only, don't send", action='store_true')
    parser.add_argument('--iothubcreds', type=str, help=f"IoT Hub credentials file",default=os.path.join(os.path.dirname(__file__),f"creds_iothub_{os.environ['USERNAME']}.json"))
    args = parser.parse_args(argv[1:])

    device_id=args.deviceId
    csv_file=args.csvFile
    if csv_file is None:
        csv_file=device_id
        device_id=csv_file.split('.')[-2].split('-@')[-1]
    messages_to_send=args.count
    offset=args.offset
    with io.open(args.iothubcreds) as f:
        iothub_creds = json.load(f)
    iot_hub_name=iothub_creds['iot_hub_name']
    access_key=iothub_creds['devices_access_keys'][device_id][0]


    print(f"Using CSV file: {csv_file} and device {device_id}")

    # The connection string for a device should never be stored in code. For the sake of simplicity we're using an environment variable here.
    conn_str=f"HostName={iot_hub_name}.azure-devices.net;DeviceId={device_id};SharedAccessKey={access_key}"

    print(f"Connecting to {iot_hub_name}, about to send {messages_to_send} messages from offset {offset}")

    # The client object is used to interact with your Azure IoT hub.
    device_client = azure.iot.device.aio.IoTHubDeviceClient.create_from_connection_string(conn_str)

    import pandas as pd
    #df=pd.read_csv(csv_file,parse_dates=['date'],infer_datetime_format=True,skiprows=offset,nrows=messages_to_send)
    df=pd.read_csv(csv_file,parse_dates=None,infer_datetime_format=False,nrows=offset+messages_to_send)

    # drop the offset first rows
    df=df.iloc[offset:]
    print(f"Got data frame of shape {df.shape}")

    original_columns=df.columns

    # Rebase date column, Add a column with rebased timestamps
    date_field=args.dateField
    if date_field is None:
        date_field=df.columns[0]
    print(f"Using date_field={date_field}")
    # Convert date to rebase it
    df[date_field]=pd.to_datetime(df[date_field])
    from datetime import datetime

    # Note: if we are not pacing, we send all records at once, projected in the past up to the latest (max)
    # If we are pacing, we rebase from the earliest (min) and increment from there
    deltaTS=datetime.utcnow()-(df[date_field].min() if args.pace else df[date_field].max())

    print(f"Rebasing timestamp to delta {deltaTS}")
    if not args.norebase:
        df[date_field]=deltaTS+df[date_field]
    # keep rebased column
    df[f"rebase_{date_field}"]=df[date_field]
    # convert back to string format otherwise cannot serialize with JSON
    df[date_field]=df[date_field].astype(str)
    print(df.head(2))
    print("...........")
    print(df.tail(2))

    # Connect the client.
    await device_client.connect()

    async def send_message(df,i,ts):
        import json,uuid
        import collections

        # Create a JSON string representation with keys in the same order as the CSV columns
        msg=df[original_columns].iloc[i].to_dict(into=collections.OrderedDict)
        msg[date_field]=str(ts)
        msg = azure.iot.device.Message(json.dumps(msg))
        msg.message_id = uuid.uuid4()
        msg.correlation_id = f"corr{msg.message_id}"
        # msg.custom_properties["tornado-warning"] = "yes"
        if not args.test:
            print(f"Sending message #{str(1+i)} at {datetime.utcnow()} {type(msg)} {msg}")
            await device_client.send_message(msg)
        else:
            print(f"Would send message #{str(1+i)} at {datetime.utcnow()} {type(msg)} {msg}")

    # send `messages_to_send` messages in sequence with pacing
    #await asyncio.gather(*[send_message(df,i) for i in range(messages_to_send)])
    import time
    last_ts=None
    last_deltaWaitSec=None
    for i in range(messages_to_send):
        ts=df.iloc[i][f"rebase_{date_field}"]
        if args.pace and last_ts is not None:
            deltaWaitSec=int((ts-last_ts).total_seconds())
            if last_deltaWaitSec is not None and deltaWaitSec > int(last_deltaWaitSec * args.maxDelayMult):
                print(f"Time gap {deltaWaitSec} > {last_deltaWaitSec}*{args.maxDelayMult}, clipping to {int(last_deltaWaitSec * args.maxDelayMult)}")
                deltaWaitSec=int(last_deltaWaitSec * args.maxDelayMult)
            print(f"Waiting for {deltaWaitSec:d} seconds at {datetime.utcnow()}")
            time.sleep(deltaWaitSec)
            last_deltaWaitSec=deltaWaitSec
            ts=datetime.utcnow()
        await send_message(df,i,ts)
        last_ts=ts

    # finally, disconnect
    await device_client.disconnect()

if __name__ == "__main__":
    asyncio.run(main(sys.argv))

    # If using Python 3.6 or below, use the following code instead of asyncio.run(main()):
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    # loop.close()
