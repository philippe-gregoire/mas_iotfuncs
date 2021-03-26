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
POINT_PREFIX='Modbus1.1.Holding16.'

POINT_ATTR_MAP={
    # X Axis Sensor 1 
    f'{POINT_PREFIX}0':('X1','globalv'),
    f'{POINT_PREFIX}2':('X1','globalg'),
    f'{POINT_PREFIX}4':('X1','fftv1'),
    f'{POINT_PREFIX}6':('X1','fftg1'),
    f'{POINT_PREFIX}8':('X1','fftv2'),
    f'{POINT_PREFIX}10':('X1','fftg2'),
    f'{POINT_PREFIX}12':('X1','fftv3'),
    f'{POINT_PREFIX}14':('X1','fftg3'),
    f'{POINT_PREFIX}16':('X1','fftv4'),
    f'{POINT_PREFIX}18':('X1','fftg4'),
    f'{POINT_PREFIX}20':('X1','fftv5'),
    f'{POINT_PREFIX}22':('X1','fftg5'),
    f'{POINT_PREFIX}24':('X1','fftv6'),
    f'{POINT_PREFIX}26':('X1','fftg6'),
    f'{POINT_PREFIX}28':('X1','fftv7'),
    f'{POINT_PREFIX}30':('X1','fftg7'),
    f'{POINT_PREFIX}32':('X1','fftv8'),
    f'{POINT_PREFIX}34':('X1','fftg8'),
    f'{POINT_PREFIX}36':('X1','fftv7_2'),
    f'{POINT_PREFIX}38':('X1','fftg7_2'),
    f'{POINT_PREFIX}40':('X1','fftv5_7'),
    f'{POINT_PREFIX}42':('X1','fftg5_7'),
    f'{POINT_PREFIX}44':('X1','temp'),
    # Y Axis Sensor 1(
    f'{POINT_PREFIX}46':('Y1','globalv'),
    f'{POINT_PREFIX}48':('Y1','globalg'),
    f'{POINT_PREFIX}50':('Y1','fftv1'),
    f'{POINT_PREFIX}52':('Y1','fftg1'),
    f'{POINT_PREFIX}54':('Y1','fftv2'),
    f'{POINT_PREFIX}56':('Y1','fftg2'),
    f'{POINT_PREFIX}58':('Y1','fftv3'),
    f'{POINT_PREFIX}60':('Y1','fftg3'),
    f'{POINT_PREFIX}62':('Y1','fftv4'),
    f'{POINT_PREFIX}64':('Y1','fftg4'),
    f'{POINT_PREFIX}66':('Y1','fftv5'),
    f'{POINT_PREFIX}68':('Y1','fftg5'),
    f'{POINT_PREFIX}70':('Y1','fftv6'),
    f'{POINT_PREFIX}72':('Y1','fftg6'),
    f'{POINT_PREFIX}74':('Y1','fftv7'),
    f'{POINT_PREFIX}76':('Y1','fftg7'),
    f'{POINT_PREFIX}78':('Y1','fftv8'),
    f'{POINT_PREFIX}80':('Y1','fftg8'),
    f'{POINT_PREFIX}82':('Y1','fftv7_2'),
    f'{POINT_PREFIX}84':('Y1','fftg7_2'),
    f'{POINT_PREFIX}86':('Y1','fftv5_7'),
    f'{POINT_PREFIX}88':('Y1','fftg5_7'),
    f'{POINT_PREFIX}90':('Y1','temp'),
    # X Axis sensor 2)
    f'{POINT_PREFIX}92':('X2','globalv'),
    f'{POINT_PREFIX}94':('X2','globalg'),
    f'{POINT_PREFIX}96':('X2','fftv1'),
    f'{POINT_PREFIX}98':('X2','fftg1'),
    f'{POINT_PREFIX}100':('X2','fftv2'),
    f'{POINT_PREFIX}102':('X2','fftg2'),
    f'{POINT_PREFIX}104':('X2','fftv3'),
    f'{POINT_PREFIX}106':('X2','fftg3'),
    f'{POINT_PREFIX}108':('X2','fftv4'),
    f'{POINT_PREFIX}110':('X2','fftg4'),
    f'{POINT_PREFIX}112':('X2','fftv5'),
    f'{POINT_PREFIX}114':('X2','fftg5'),
    f'{POINT_PREFIX}116':('X2','fftv6'),
    f'{POINT_PREFIX}118':('X2','fftg6'),
    f'{POINT_PREFIX}120':('X2','fftv7'),
    f'{POINT_PREFIX}122':('X2','fftg7'),
    f'{POINT_PREFIX}124':('X2','fftv8'),
    f'{POINT_PREFIX}126':('X2','fftg8'),
    f'{POINT_PREFIX}128':('X2','fftv7_2'),
    f'{POINT_PREFIX}130':('X2','fftg7_2'),
    f'{POINT_PREFIX}132':('X2','fftv5_7'),
    f'{POINT_PREFIX}134':('X2','fftg5_7'),
    f'{POINT_PREFIX}136':('X2','temp'),
    f'{POINT_PREFIX}138':('Motor','rpm')
}

DEVICE_ATTR='deviceid'

class PhGOSIElemsPreload(func_base.PhGCommonPreload):
    """
    OSIPIElementsPreload
    Do an OSI Pi Server read as a preload activity. Load results into the Entity Type time series table.
    """

    def __init__(self, osipi_host, osipi_port, osipi_user, osipi_pass, 
                 database_path, element_name,
                 date_field,
                 osipi_elements_preload_ok):
        super().__init__(osipi_preload_ok,f"osipi_lastseq_{name_filter.lower()}",str)

        import argparse

        # create an instance variable with the same name as each arg
        self.osipi_host = osipi_host
        self.osipi_port = osipi_port
        self.osipi_user = osipi_user
        self.osipi_pass = osipi_pass
        self.srvParams=argparse.Namespace(pihost=osipi_host,piport=osipi_port,piuser=osipi_user,pipass=osipi_pass)
        self.database_path = database_path
        self.element_name = element_name
        self.date_field=date_field.strip()

        self.osipi_elements_preload_ok=osipi_elements_preload_ok

    @classmethod
    def build_ui(cls):
        """
        Describe the OSIPi Server connectivity
        """
        from iotfunctions import ui

        # define arguments that behave as function inputs
        inputs = [
            ui.UISingle(required=True, datatype=str, name='osipi_host', description='OSIPi server hostname'),
            ui.UISingle(required=True, datatype=int, name='osipi_port', description='OSIPi server host port'),
            ui.UISingle(required=True, datatype=str, name='osipi_user', description='OSIPi server userid'),
            ui.UISingle(required=True, datatype=str, name='osipi_pass', description='OSIPi server password'),
            ui.UISingle(required=True, datatype=str, name='database_path', description='OSIPi Element database Path'),
            ui.UISingle(required=True, datatype=str, name='element_name', description='OSIPi Parent Element Name'),
            ui.UISingle(required=True, datatype=str, name='date_field', description='Field in the incoming JSON for event date (timestamp)', default='date'),
            # ui.UISingle(required=True, datatype=str, name='required_fields', description='Fields in the incoming JSON that are required for the payload to be retained'),
        ]

        # define arguments that behave as function outputs
        outputs = [
            ui.UIStatusFlag(name='osipi_elements_preload_ok')
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

        ''' Test the pi Points get function '''
        from phg_iotfuncs.osipiutils import ATTR_FIELDS,getOSIPiElements,convertToEntities
    
        # Get the specified Points attributes fields from OSIServer
        elemVals=getOSIPiElements(self.srvParams,self.database_path,self.element_name,ATTR_FIELDS,DEVICE_ATTR)

        # If no records, return immediately
        if len(elemVals)==0:
            logger.warning(f"No messages returned from OSIPi")
            return False
        logger.info(f"Retrieved messages for {len(elemVals)} attributes")
       
        # Get into DataFrame table form indexed by timestamp 
        df=convertToEntities(elemVals,self.date_field,DEVICE_ATTR)

        # Store the highest sequence number
        max_timestamp=df[self.date_field].max()
        logger.info(f"Highest timestamp={max_timestamp} of type {type(max_timestamp)}")

        self.storePreload(db,table,entity_type,entity_meta_dict,df)

        # update sequence number, use global constant
        self.updateLastSeq(db,str(max_timestamp))

        return True
class PhGOSIPIPointsPreload(func_base.PhGCommonPreload):
    """
    OSIPIPointsPreload
    Do an OSI Pi Server read as a preload activity. Load results into the Entity Type time series table.
    """

    def __init__(self, osipi_host, osipi_port, osipi_user, osipi_pass, 
                 name_filter, date_field,
                 osipi_preload_ok):
        super().__init__(osipi_preload_ok,f"osipi_lastseq_{name_filter.lower()}",str)

        import argparse

        # create an instance variable with the same name as each arg
        self.osipi_host = osipi_host
        self.osipi_port = osipi_port
        self.osipi_user = osipi_user
        self.osipi_pass = osipi_pass
        self.srvParams=argparse.Namespace(pihost=osipi_host,piport=osipi_port,piuser=osipi_user,pipass=osipi_pass)
        self.name_filter = name_filter
        self.date_field=date_field.strip()
        # Make a set out of the required fields plus date
        # self.required_fields={r.strip() for r in required_fields.split(',')} | {self.date_field}

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
            ui.UISingle(required=True, datatype=int, name='osipi_port', description='OSIPi server host port'),
            ui.UISingle(required=True, datatype=str, name='osipi_user', description='OSIPi server userid'),
            ui.UISingle(required=True, datatype=str, name='osipi_pass', description='OSIPi server password'),
            ui.UISingle(required=True, datatype=str, name='name_filter', description='OSIPi Point name filter'),
            ui.UISingle(required=True, datatype=str, name='date_field', description='Field in the incoming JSON for event date (timestamp)', default='date'),
            # ui.UISingle(required=True, datatype=str, name='required_fields', description='Fields in the incoming JSON that are required for the payload to be retained'),
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

        ''' Test the pi Points get function '''
        from phg_iotfuncs.osipiutils import ATTR_FIELDS,getOSIPiPoints,mapPointValues,convertToEntities
    
        # Get the specified Points attributes fields from OSIServer
        ptVals=getOSIPiPoints(self.srvParams,self.name_filter,ATTR_FIELDS)
    
        # If no records, return immediately
        if len(ptVals)==0:
            logger.warning(f"No messages returned from OSIPi")
            return False
        logger.info(f"Retrieved messages for {len(ptVals)} attributes")
        # Map Point values to a flattened version indexed by (deviceID,timestamp)
        flattened=mapPointValues(ptVals,DEVICE_ATTR,POINT_ATTR_MAP)
        
        # Get into DataFrame table form indexed by timestamp 
        df=convertToEntities(flattened,self.date_field,DEVICE_ATTR)

        # Store the highest sequence number
        max_timestamp=df[self.date_field].max()
        logger.info(f"Highest timestamp={max_timestamp} of type {type(max_timestamp)}")

        self.storePreload(db,table,entity_type,entity_meta_dict,df)

        # update sequence number, use global constant
        self.updateLastSeq(db,str(max_timestamp))

        return True