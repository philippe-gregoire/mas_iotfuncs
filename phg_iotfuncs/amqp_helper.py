# *****************************************************************************
# © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# AMQP helper functions
#
# Written by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe
# *****************************************************************************
# See https://github.com/MicrosoftDocs/azure-docs/blob/master/articles/iot-hub/iot-hub-amqp-support.md
# and https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-amqp-support
# https://azuresdkdocs.blob.core.windows.net/$web/python/azure-eventhub/5.0.0b5/_modules/azure/eventhub/common.html

import logging
logger = logging.getLogger(__name__)

def generate_sas_token(uri, key, policy_name, expiry=3600):
    '''
    Adapted from azure sample helper module
    '''
    import base64, hashlib, urllib.parse, hmac
    from time import time

    # Set time to live from current time
    ttl = int(time() + expiry)
    rawtoken = {'sr':  uri,
                'sig': base64.b64encode(hmac.HMAC(base64.b64decode(key),f"{urllib.parse.quote_plus(uri)}\n{ttl}".encode('utf-8'), hashlib.sha256).digest()),
                'se': str(ttl)}

    if policy_name is not None:
        rawtoken['skn'] = policy_name
    logger.info(f"SharedAccessSignature {urllib.parse.urlencode(rawtoken)}" )
    return f"SharedAccessSignature {urllib.parse.urlencode(rawtoken)}"

# Optional filtering predicates can be specified by using endpoint_filter
# Valid predicates include:
# - amqp.annotation.x-opt-sequence-number
# - amqp.annotation.x-opt-offset
# - amqp.annotation.x-opt-enqueued-time
# Set endpoint_filter variable to None if no filter is needed

def endpoint_uri(iot_hub_name,consumer_group,partition_id,policy_name,access_key,filter=None):
    import urllib.parse

    hostname = f"{iot_hub_name}.azure-devices.net"
    operation = f"/messages/events/ConsumerGroups/{consumer_group}/Partitions/{partition_id}"
    username = f"{policy_name}@sas.root.{iot_hub_name}"
    sas_token = generate_sas_token(hostname, access_key, policy_name)

    return uri_filter(f"amqps://{urllib.parse.quote_plus(username)}:{urllib.parse.quote_plus(sas_token)}@{hostname}{operation}",filter)

def make_fiter(since_seq,max_age_sec,since_ts,device_id):
    '''
       Create a AMQP (MessageHub) Query filter
       Syntax described in https://docs.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-routing-query-syntax
       Only some attributes can be set, as per https://github.com/Azure/amqpnetlite/blob/master/docs/articles/azure_eventhubs.md
    '''
    filters=[]
    if since_seq is not None and int(since_seq)>0:
        filters.append(f"amqp.annotation.x-opt-sequence-number >= {since_seq}")

    if max_age_sec is not None and int(max_age_sec)>0:
        from time import time
        filters.append(f"amqp.annotation.x-opt-enqueued-time >= {int(time()-int(max_age_sec))}000")

    if since_ts is not None:
        from datetime import datetime
        if isinstance(since_ts,str):
            since_ts=datetime.fromisoformat(since_ts)
        if  isinstance(since_ts,datetime):
            since_ts=int(since_ts.timestamp()*1000)
        filters.append(f"amqp.annotation.x-opt-enqueued-time >= {since_ts}")

    if device_id is not None:
        filters.append(f"iothub-connection-device-id = '{device_id}'")

    return ' AND '.join(filters)

def uri_filter(uri,filter):
    import uamqp.address
    source_uri = uamqp.address.Source(uri)

    if filter is not None:
        logger.info(f"Setting filter {filter}")
        source_uri.set_filter(filter)

    return source_uri

def processMessage(msg):
    from datetime import datetime
    import json

    msgData=''.join([x.decode() for x in msg.get_data()])
    try:
        msgDict=json.loads(msgData)
        logger.debug(f"got json of type {type(msgDict)}: {msgDict} from {msgData}")
        if not isinstance(msgDict,dict):
            msgDict={'json':msgDict}
    except json.decoder.JSONDecodeError as exc:
        # Could not parse as json, set as rawData
        msgDict={'rawData':msgData}

    # Add the annotation keys, converted from binary to str
    msgDict.update({k.decode():(a.decode() if isinstance(a,bytes) else a) for k,a in msg.annotations.items()})

    # Fix the date key
    if 'x-opt-enqueued-time' in msgDict:
        msgDict['x-opt-enqueued-time']=datetime.utcfromtimestamp(int(msgDict['x-opt-enqueued-time'])/1000)

    return msgDict

def amqpReceive(iot_hub_name,policy_name,consumer_group,partition_id,access_key,since_seq=None,max_age_sec=None,since_ts=None,device_id=None,max_batch_size=60,timeout=1000,debug_network=False):
    '''
    Receive messages from from AMQP
    The optional parameters `since_seq`, `max_age_sec`, `since_ts` are optional and mutually exclusive, in this precedence order
    '''
    import uamqp

    filter=make_fiter(since_seq,max_age_sec,since_ts,device_id)
    source_uri=endpoint_uri(iot_hub_name,consumer_group,partition_id,policy_name,access_key,filter)

    logger.info(f"Receiving from {source_uri}")
    batch=[]

    try:
        receive_client = uamqp.ReceiveClient(source_uri, debug=debug_network)

        logger.info("Start receiving messages batch")
        # receive_client.receive_messages(processMessage)
        batch=receive_client.receive_message_batch(max_batch_size=max_batch_size,timeout=timeout)

    except uamqp.errors.LinkRedirect as redirect:
        logger.info("Redirect exception, following")
        # receive_client.redirect(redirect,uamqp.authentication.SASTokenAuth.from_shared_access_key(redirect.address.decode(), policy_name, access_key))
        receive_client.close()

        sas_auth = uamqp.authentication.SASTokenAuth.from_shared_access_key(redirect.address.decode(), policy_name, access_key)
        receive_client = uamqp.ReceiveClient(uri_filter(redirect.address,filter), auth=sas_auth, debug=True)

    except uamqp.errors.ConnectionClose as connClose:
        logger.info(f"Connection close {connClose}")
        batch=None

    messages=[]
    while batch!=None:
        logger.info(f"Got {len(batch)} messages")
        for msg in batch:
            messages.append(processMessage(msg))

        try:
            # Receiving next messages in batch
            logger.debug("Receiving next messages")

            # receive_client.receive_messages(processMessage)
            batch=receive_client.receive_message_batch(max_batch_size=max_batch_size,timeout=timeout)

            if len(batch)==0:
                batch=None

        except uamqp.errors.LinkDetach as detach:
            logger.info(f"Link Detach {detach}")
            logger.info(f"detach.condition={detach.condition}")
            logger.info(f"detach.info={detach.info}")
            logger.info(f"detach.description={detach.description}")
            batch=None
        except uamqp.errors.ConnectionClose as connClose:
            logger.info(f"Connection close {connClose}")
            batch=None

    receive_client.close()

    return messages
