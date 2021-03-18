# *****************************************************************************
# © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
#
# Maximo Monitor IoT Preload Functions to retrieve Entity data feed from 
# AMQP, typically from an AzureIoT hub
#
# Written by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe
# *****************************************************************************

import os, io, json, importlib, fnmatch
import logging,pprint

from iotfunctions.base import BaseTransformer, BaseDataSource, BasePreload
import iotfunctions.db
from phg_iotfuncs import func_base

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install
# If forking to a private repository, an access token must be generated
# This is a Personal Access Token for the GitHub ID, who has been authorized to the git project repository
#token='xyz'
#PACKAGE_URL = f"git+https://{token}@github.com/philippe-gregoire/mas_iotfuncs@master"

PACKAGE_URL = f"git+https://github.com/philippe-gregoire/mas_iotfuncs@master"

class PhGAMQPPreload(func_base.PhGCommonPreload):
    """
    AMQPPreload
    Do an AMQP read as a preload activity. Load results into the Entity Type time series table.
    """

    def __init__(self, iot_hub_name, policy_name, consumer_group, partition_id, access_key,
                    device_id,date_field,required_fields,
                    amqp_preload_ok):
        super().__init__(amqp_preload_ok)

        # Turn amqp logging to Warning
        logging.getLogger('uamqp').setLevel(logging.WARNING)

        # create an instance variable with the same name as each arg
        self.iot_hub_name = iot_hub_name
        self.policy_name = policy_name
        self.consumer_group = consumer_group
        self.partition_id = partition_id
        self.access_key = access_key
        self.device_id = device_id
        self.date_field=date_field.strip()
        # Make a set out of the required fields plus date
        self.required_fields={r.strip() for r in required_fields.split(',')} | {self.date_field}
        self.lastseq_constant=f"amqp_lastseq_{device_id.lower()}"

        self.amqp_preload_ok=amqp_preload_ok

    @classmethod
    def build_ui(cls):
        """
        Describe the Azure IoT connectivity
        """
        from iotfunctions import ui

        # define arguments that behave as function inputs
        inputs = [
            ui.UISingle(required=True, datatype=str, name='iot_hub_name', description='Azure IoT Hub Name'),
            ui.UISingle(required=True, datatype=str, name='policy_name', description='Azure IoT Policy'),
            ui.UISingle(required=True, datatype=str, name='consumer_group', description='Azure IoT Consumer Group'),
            ui.UISingle(required=True, datatype=int, name='partition_id', description='Azure IoT Partition ID'),
            ui.UISingle(required=True, datatype=str, name='access_key', description='Azure IoT Secret Access Key'),
            ui.UISingle(required=True, datatype=str, name='device_id', description='Azure IoT Device ID'),
            ui.UISingle(required=True, datatype=str, name='date_field', description='Field in the incoming JSON for event date (timestamp)', default='date'),
            ui.UISingle(required=True, datatype=str, name='required_fields', description='Fields in the incoming JSON that are required for the payload to be retained'),
        ]

        # define arguments that behave as function outputs
        outputs = [
            ui.UIStatusFlag(name='amqp_preload_ok')
        ]
        return (inputs, outputs)

    @classmethod
    def get_module_files(cls,pattern):
        module_path=os.path.dirname(importlib.import_module(cls.__module__).__file__)
        logger.debug(f"module_path={module_path}")
        return [f for f in os.listdir(module_path) if fnmatch.fnmatch(f,pattern)],module_path

    def preload(self,entity_type,db,table,entityMetaDict,params,entity_meta_dict,last_seq):
        """
            Implement the preload code
        """
        import iotfunctions.metadata
        from phg_iotfuncs import iotf_utils
        import numpy as np, pandas as pd
        import datetime as dt

        # Turn amqp logging to Warning
        logging.getLogger('uamqp').setLevel(logging.WARNING)

        # Get data from IoT Event Hub
        from phg_iotfuncs import amqp_helper
        msgs=amqp_helper.amqpReceive(self.iot_hub_name,self.policy_name,self.consumer_group,self.partition_id,self.access_key,timeout=1000,since_seq=last_seq+1)

        # If no records, return imediatelly
        if len(msgs)==0:
            logger.warning(f"No messages returned from AMQP")
            return False
        logger.info(f"Retrieved {len(msgs)} messages")

        # Keep only records that are for the matching device_id
        msgs=[m for m in msgs if self.device_id == m['iothub-connection-device-id']]
        if len(msgs)==0:
            logger.warning(f"No messages retained after filtering  iothub-connection-device-id=={self.device_id}")
            return False
        logger.info(f"Keeping {len(msgs)} records after keeping iothub-connection-device-id=={self.device_id}")

        # Filter the json records to keep only those that have the required keys
        msgs=[m for m in msgs if self.required_fields <= m.keys()]

        # If no records, return imediatelly
        if len(msgs)==0:
            logger.warning(f"No messages retained after filtering records without {self.required_fields}")
            return False
        logger.info(f"Keeping {len(msgs)} records after filtering out records without {self.required_fields}")

        # We get the messages in an array of dicts, convert to dataframe
        df=pd.DataFrame.from_dict(msgs)
        logger.info(f"df initial columns={[c for c in df.columns]}")

        # Find the date column. We know at this stage that the records we keep have a date_field
        df[self.date_field]=pd.to_datetime(df[self.date_field],errors='coerce')

        # Adjust columns, add index columns deviceid, rcv_timestamp_utc
        id_index_col='deviceid'
        ts_index_col='rcv_timestamp_utc'    # Column which holds the timestamp part of the index
        logger.info(f"entity_type._timestamp={entity_type._timestamp}")
        logger.info(f"Using columns [iothub-connection-device-id,{self.date_field}] as index [{id_index_col},{ts_index_col}]")
        df.rename(columns={'iothub-connection-device-id':id_index_col,self.date_field:ts_index_col},inplace=True)
        df.set_index([id_index_col,ts_index_col],drop=False,inplace=True)

        # Give back the timestamp column its original name
        df.rename(columns={ts_index_col:self.date_field},inplace=True)

        # Adjust column names, set updated_utc to current ts
        df.rename(columns={'iothub-message-source':'eventtype'},inplace=True)
        df['updated_utc']=dt.datetime.utcnow()

        # Store the highest sequence number
        max_sequence_number=df['x-opt-sequence-number'].max()
        logger.info(f"Highest seq number={max_sequence_number}")

        # drop metadata columns
        df.drop(columns=[c for c in df.columns if c.startswith('iothub-')],inplace=True)
        df.drop(columns=[c for c in df.columns if c.startswith('x-opt-')],inplace=True)

        logger.info(f"df columns after metadata drop={[c for c in df.columns]}")

        self.storePreload(db,table,entity_type,entity_meta_dict,df)

        # update sequence number, use global constant
        self.updateLastSeq(db,max_sequence_number)

        return True

class PhGSBPreload(phg_iotfuncs.PhGCommonPreload):
    """ SBPreload - Not functional yet
        Azure ServiceBus Preload function
        Load results into the Entity Type time series table.
    """

    def __init__(self, EndPoint, SharedAccessKeyName, SharedAccessKey, EntityPath,
                 sb_preload_ok):
        super().__init__(sb_preload_ok)

        # create an instance variable with the same name as each arg
        self.EndPoint = EndPoint
        self.SharedAccessKeyName = SharedAccessKeyName
        self.SharedAccessKey = SharedAccessKey
        self.EntityPath = EntityPath
        self.ConsumerGroup='$Default'
        self.Partition = 0
        self.lastseq_constant=f"sb_lastseq_{SharedAccessKeyName.lower()}"

        self.sb_preload_ok=sb_preload_ok

    @classmethod
    def build_ui(cls):
        """
        Describe the Azure ServiceBus connectivity
        """
        from iotfunctions import ui

        # define arguments that behave as function inputs
        inputs = [
            ui.UISingle(required=True, datatype=str, name='EndPoint', description='Azure EndPoint'),
            ui.UISingle(required=True, datatype=str, name='SharedAccessKeyName', description='Azure SharedAccessKeyName'),
            ui.UISingle(required=True, datatype=str, name='SharedAccessKey', description='Azure SharedAccessKey'),
            ui.UISingle(required=True, datatype=int, name='EntityPath', description='Azure EntityPath'),
        ]

        # define arguments that behave as function outputs
        outputs = [
            ui.UIStatusFlag(name='sb_preload_ok')
        ]
        return (inputs, outputs)

    def preload(entity_type,db,table,entityMetaDict,params,entity_meta_dict,last_seq):
        """
        Default preload call-back
        """
        import iotfunctions.metadata
        from phg_iotfuncs import iotf_utils
        import numpy as np, pandas as pd
        import datetime as dt

        return False
