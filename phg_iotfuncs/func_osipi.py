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
        super().__init__(osipi_elements_preload_ok,f"osipi_lastseq_{element_name.lower()}",str)

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
        import numpy as np, pandas as pd
        import datetime as dt

        # Import our utilities for IoTF
        from phg_iotfuncs import iotf_utils

        # Import OSIPi helper utilities
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

        # Extract the highest sequence number
        max_timestamp=df[self.date_field].max()
        logger.info(f"Highest timestamp={max_timestamp} of type {type(max_timestamp)}")

        # Map column names for special characters
        logger.info(f"Columns 2-before {df.columns}")
        df.rename(columns={c:iotf_utils.toMonitorColumnName(c) for c in df.columns},inplace=True)
        logger.info(f"Columns 2-after {df.columns}")

        self.storePreload(db,table,entity_type,entity_meta_dict,df)

        # update sequence number, use global constant
        self.updateLastSeq(db,str(max_timestamp))

        return True

class PhGOSIPIPointsPreload(func_base.PhGCommonPreload):
    """
    OSIPIPointsPreload
    Do an OSI Pi Server read as a preload activity. Load results into the Entity Type time series table.
    The mapping from Points name to Entity columns values is made through a JSON input map
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
        self.points_attr_map = points_attr_map
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
            ui.UIParameters(required=True, name='points_attr_map', description='OSIPi Points names to attribute names map'),
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
        flattened=mapPointValues(ptVals,DEVICE_ATTR,self.points_attr_map)
        
        # Get into DataFrame table form indexed by timestamp 
        df=convertToEntities(flattened,self.date_field,DEVICE_ATTR)

        # Store the highest sequence number
        max_timestamp=df[self.date_field].max()
        logger.info(f"Highest timestamp={max_timestamp} of type {type(max_timestamp)}")

        self.storePreload(db,table,entity_type,entity_meta_dict,df)

        # update sequence number, use global constant
        self.updateLastSeq(db,str(max_timestamp))

        return True