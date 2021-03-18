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
# an OSISoft PiVision server using the OSIPi REST API
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

## Hardcoded values, will parametrize later
# Map of points to attributes
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
    'Modbus1.1.Holding16.138':('','rpm')
}

DEVICE_ATTR='deviceid'

# List the fields we want to retrieve for Points
POINTS_FIELDS=['Name'] # other fields are empty
# List the values attributes we want to retrieve
VALUE_FIELDS=['Value','Timestamp']

class PhGOSIPIPreload(func_base.PhGCommonPreload):
    """
    OSIPIPreload
    Do an OSI Pi Server read as a preload activity. Load results into the Entity Type time series table.
    """

    def __init__(self, osipihost, osipiport, osipiuser, osipipass, 
                 nameFilter,
                 date_field,required_fields,
                 osipi_preload_ok):
        super().__init__(osipi_preload_ok)

        # create an instance variable with the same name as each arg
        self.osipi_host = osipi_host
        self.osipi_port = osipi_host
        self.osipi_user = osipi_user
        self.osipi_pass = osipi_pass
        self.name_filter = name_filter
        self.date_field=date_field.strip()
        # Make a set out of the required fields plus date
        self.required_fields={r.strip() for r in required_fields.split(',')} | {self.date_field}
        self.lastseq_constant=f"osipi_lastseq_{nameFilter.lower()}"

        self.osipi_preload_ok=osipi_preload_ok

    @classmethod
    def build_ui(cls):
        """
        Describe the OSIPi Server connectivity
        """
        from iotfunctions import ui

        # define arguments that behave as function inputs
        inputs = [
            ui.UISingle(required=True, datatype=str, name='osipi_host', description='OSIPi server hostname'),
            ui.UISingle(required=True, datatype=str, name='osipi_host', description='OSIPi server host port'),
            ui.UISingle(required=True, datatype=str, name='osipi_user', description='OSIPi server userid),
            ui.UISingle(required=True, datatype=int, name='osipi_pass', description='OSIPi server password'),
            ui.UISingle(required=True, datatype=str, name='name_filter', description='OSIPi Point name filter'),
            ui.UISingle(required=True, datatype=str, name='date_field', description='Field in the incoming JSON for event date (timestamp)', default='date'),
            ui.UISingle(required=True, datatype=str, name='required_fields', description='Fields in the incoming JSON that are required for the payload to be retained'),
        ]

        # define arguments that behave as function outputs
        outputs = [
            ui.UIStatusFlag(name='osipi_preload_ok')
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

        # Get data from IoT Event Hub
        from phg_iotfuncs import osipiutils

        msgs=osipiutils.getOSIPiPoints(self.osipi_host,self.osipi_port,self.osipi_user,self.osipi_pass,self.name_filter,POINTS_FIELDS,VALUE_FIELDS)

        # If no records, return imediatelly
        if len(msgs)==0:
            logger.warning(f"No messages returned from OSIPi")
            return False
        logger.info(f"Retrieved messages for {len(msgs)} attributes")

        # Map values to a flattened version indexed by timestamp
        mapped=osipiutils.mapValues(msgs,DEVICE_ATTR,POINT_ATTR_MAP)

        # We get the messages in an array of dicts, convert to dataframe
        df=pd.DataFrame.from_records([v for v in msgs.values()])
        logger.info(f"df initial columns={[c for c in df.columns]}")

        # Find the date column. We know at this stage that the records we keep have a date_field
        df[self.date_field]=pd.to_datetime(df[self.date_field],errors='coerce')

        # Adjust columns, add index columns deviceid, rcv_timestamp_utc
        id_index_col=DEVICE_ATTR
        ts_index_col='rcv_timestamp_utc'    # Column which holds the timestamp part of the index
        logger.info(f"entity_type._timestamp={entity_type._timestamp}")
        logger.info(f"Using columns [{DEVICE_ATTR},{self.date_field}] as index [{id_index_col},{ts_index_col}]")
        df.rename(columns={self.date_field:ts_index_col},inplace=True)
        df.set_index([id_index_col,ts_index_col],drop=False,inplace=True)

        # Give back the timestamp column its original name
        df.rename(columns={ts_index_col:self.date_field},inplace=True)

        # Adjust column names, set updated_utc to current ts
        # df.rename(columns={'iothub-message-source':'eventtype'},inplace=True)
        df['updated_utc']=dt.datetime.utcnow()

        # Store the highest sequence number
        max_sequence_number=df[self.date_field].max()
        logger.info(f"Highest seq number={max_sequence_number}")

        self.storePreload(db,table,entity_type,entity_meta_dict,df)

        # update sequence number, use global constant
        self.updateLastSeq(db,max_sequence_number)

        return True