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
# Maximo Monitor IoT Functions Preload Base Class
#
# Written by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe
# *****************************************************************************

import os, io, json, importlib, fnmatch
import logging,pprint

from iotfunctions.base import BaseTransformer, BaseDataSource, BasePreload, BaseFilter
import iotfunctions.db

from phg_iotfuncs import iotf_utils

logger = logging.getLogger(__name__)

PACKAGE_URL = f"git+https://github.com/philippe-gregoire/mas_iotfuncs@master"

class PhGCommonPreload(BasePreload):
    """
    CommonPreload
    Abstract base class with common code for all preload.
    Takes care of making available the various variables needed to implement 
    preload activities in the preload() method
    """
    def __init__(self,preload_ok,lastseq_constant,lastseq_type=str,lastseq_init='-1'):
        super().__init__(dummy_items=[], output_item=preload_ok)
        self.lastseq_constant=lastseq_constant
        self.lastseq_type=lastseq_type
        self.lastseq_init=lastseq_init

        self.logger=logging.getLogger(self.__class__.__name__)

    @classmethod
    def get_module_files(cls,pattern):
        module_path=os.path.dirname(importlib.import_module(cls.__module__).__file__)
        logging.getLogger(cls.__name__).info(f"class {cls.__name__} module_path={module_path}")
        return [f for f in os.listdir(module_path) if fnmatch.fnmatch(f,pattern)],module_path

    def execute(self, df, start_ts=None, end_ts=None, entities=None):
        ''' When extending this class, do not override execute(), but implement preload()
        '''
        import iotfunctions.metadata
        
        import numpy as np, pandas as pd
        import datetime as dt

        # Extract useful values
        entity_type = self.get_entity_type()
        self.logger.info(f"entity_type name={entity_type.name} logical_name={entity_type.logical_name}")

        db = entity_type.db
        self.logger.debug(f"entity_type_metadata keys={db.entity_type_metadata.keys()} ")

        # get entity metadata
        entityMetaDict=db.entity_type_metadata[entity_type.name] if entity_type.name in db.entity_type_metadata else db.entity_type_metadata[entity_type.logical_name]
        self.logger.debug(f"Got entityMetaDict of type {type(entityMetaDict)} value={entityMetaDict}")

        params,entity_meta_dict=iotfunctions.metadata.retrieve_entity_type_metadata(_db=db,logical_name=entity_type.logical_name)
        self.logger.debug(f"Retrieved entity_meta of type {type(entity_meta_dict)}")
        self.logger.debug(pprint.pformat(entity_meta_dict))

        # get global constant (Current bug with entity-constant)
        last_seq=iotf_utils.getConstant(entity_type.db,self.lastseq_constant,self.lastseq_init,auto_register=True,const_type=self.lastseq_type)

        # This class is setup to write to the entity time series table
        table = entity_type.name

        # Call the virtual call-back to perform preload
        return self.preload(entity_type,db,table,entityMetaDict,params,entity_meta_dict,last_seq)

    def preload(self,entity_type,db,table,entityMetaDict,params,entity_meta_dict,last_seq):
        """
        """
        raise NotImplementedError("You need to override this method")

    def updateLastSeq(self,db,sequence_number):
        """
            Update the sequence number stored for the Entity
        """
        iotf_utils.putConstant(db,self.lastseq_constant,sequence_number)
        self.logger.info(f"Updated constant {self.lastseq_constant} to value {sequence_number}")

    def storePreload(self,db,entity_meta_dict,df,event_type,force_upper_columns=[]):
        """
        Store the Preload data, to be used by preload override
        """
        iotf_utils.adjustDataFrameColumns(db,entity_meta_dict,df,event_type,force_upper_columns)

        self.logger.info(f"Writing df {df.shape} to {entity_meta_dict['metricsTableName']}")
        self.write_frame(df=df, table_name=entity_meta_dict['metricsTableName'])
        self.logger.debug(f"Wrote {len(df.index)} rows to table {entity_meta_dict['schemaName']}.{entity_meta_dict['metricsTableName']}")

        return True

class PhGFilterMultiplicates(BaseFilter):
    """
    Filters out multiplicate consecutive rows (rows with same values but different timestamps)
    """

    def __init__(self, drop_if_NaN, timestamp_column, keep_timestamp, filter_set=None):
        super().__init__(dependent_items=keep_timestamp, output_item=filter_set)
        self.timestamp_column = timestamp_column
        self.drop_if_NaN = drop_if_NaN
        self.keep_timestamp = keep_timestamp
        self.filter_set = filter_set

    def filter(self, df):
        # Implement the logic
        logger.info(f"Got dataframe of len={len(df)}")
        logger.info(f"columns= {', '.join([c for c in df.columns])}")
        logger.info(df.describe(include='all'))
        # df is indexed by (id,date)

        # drop the rows for which not all the Order[1-3]_fftV|G are non-NaN
        import math
        drop_if_NaN = self.drop_if_NaN
        # drop_if_NaN = [f"Order{o}_fft{f}" for o in (1,2,3) for f in ('V','G')]
        df=df[df[drop_if_NaN].apply(lambda row: all([not math.isnan(c) for c in row]),axis=1)]

        return df

    @classmethod
    def build_ui(cls):
        from iotfunctions import ui
        import datetime
        # define arguments that behave as function inputs
        inputs = [ #ui.UIMultiItem(name='drop_if_NaN', datatype=float),
                  ui.UISingleItem(name='timestamp_column', datatype=datetime.datetime),
                  ui.UIMulti(name='keep_timestamp', datatype=str, values=['min', 'mean', 'max'])]
        return (inputs, [ui.UIStatusFlag('filter_set')])
