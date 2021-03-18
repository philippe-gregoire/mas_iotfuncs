# --------------------------------------------------------------------------
# Act as a device to send data works on Python 3.7 (but not 3.6, async.run() not there)
#
# Requires 'pip install azure-iot-device'
#
# Adaptde by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe


# You will get the connection string from your device on IoTHub
import sys, os, io, asyncio
import uuid

from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message

iot_hub_name = 'MaximoMonitor'
device_id='FeedInst'

access_key='G7d0Nwkuz7ND39tYhuN1GimH5fO7vladgH9nhI7IBfU='   # primary key for device FeedInst
#access_key='0xmlBmgJQxA9txs5qVC0Qxhm4WJBRqjB/LgzJIAo54M='   # secondary key for device FeedInst

messages_to_send = 10

async def main(argv):
    import sys

    # The connection string for a device should never be stored in code. For the sake of simplicity we're using an environment variable here.
    #conn_str = argv[1] if len(argv)>1 else os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")
    conn_str=f"HostName={iot_hub_name}.azure-devices.net;DeviceId={device_id};SharedAccessKey={access_key}"
    if conn_str:
        print(f"Connecting with {conn_str}, about to send {messages_to_send} messages")
    else:
        print("Specify a Connection String in env var IOTHUB_DEVICE_CONNECTION_STRING or as first arg")
        print("To get a device's connection string, navigate to Explorer/IoT devices, select your device, then use one of the primary or secondary connection strings")
        return

    # The client object is used to interact with your Azure IoT hub.
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)

    # Connect the client.
    await device_client.connect()

    async def send_test_message(i):
        print("sending message #" + str(i))
        msg = Message("test wind speed " + str(i))
        msg.message_id = uuid.uuid4()
        msg.correlation_id = "correlation-1234"
        msg.custom_properties["tornado-warning"] = "yes"
        await device_client.send_message(msg)
        print("done sending message #" + str(i))

    # send `messages_to_send` messages in parallel
    await asyncio.gather(*[send_test_message(i) for i in range(1, messages_to_send + 1)])

    # finally, disconnect
    await device_client.disconnect()

if __name__ == "__main__":
    asyncio.run(main(sys.argv))

    # If using Python 3.6 or below, use the following code instead of asyncio.run(main()):
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
    # loop.close()
