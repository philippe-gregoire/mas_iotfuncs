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

from iotfunctions.base import BaseTransformer, BaseDataSource, BasePreload
import iotfunctions.db

logger = logging.getLogger(__name__)


class PhGCommonPreload(BasePreload):
    """
    CommonPreload
    Abstract base class with common code for all preload.
    Takes care of making available the various variables needed to implement 
    preload activities in the preload() method
    """
    def __init__(self,preload_ok):
        super().__init__(dummy_items=[], output_item=preload_ok)

    def execute(self, df, start_ts=None, end_ts=None, entities=None):
        ''' When extending this class, do not override execute(), but implement preload()
        '''
        import iotfunctions.metadata
        from phg_iotfuncs import iotf_utils

        import numpy as np, pandas as pd
        import datetime as dt

        # Extract useful values
        entity_type = self.get_entity_type()
        logger.info(f"entity_type name={entity_type.name} logical_name={entity_type.logical_name}")

        db = entity_type.db
        logger.debug(f"entity_type_metadata keys={db.entity_type_metadata.keys()} ")

        # get entity metadata
        entityMetaDict=db.entity_type_metadata[entity_type.name] if entity_type.name in db.entity_type_metadata else db.entity_type_metadata[entity_type.logical_name]
        logger.debug(f"Got entityMetaDict of type {type(entityMetaDict)} value={entityMetaDict}")

        params,entity_meta_dict=iotfunctions.metadata.retrieve_entity_type_metadata(_db=db,logical_name=entity_type.logical_name)
        logger.debug(f"Retrieved entity_meta of type {type(entity_meta_dict)}")
        logger.debug(pprint.pformat(entity_meta_dict))

        # get global constant (Current bug with entity-constant)
        last_seq=iotf_utils.getConstant(entity_type.db,self.lastseq_constant,-1,auto_register=True,const_type=int)

        # This class is setup to write to the entity time series table
        table = entity_type.name

        # Call the virtual call-back to perform preload
        return self.preload(entity_type,db,table,entityMetaDict,params,entity_meta_dict,last_seq)

    def preload(self,entity_type,db,table,entityMetaDict,params,entity_meta_dict,last_seq):
        """
        """
        raise NotImplemented("You need to override this method")

    def updateLastSeq(self,db,sequence_number):
        """
            Update the sequence number stored for the Entity
        """
        from phg_iotfuncs import iotf_utils
        iotf_utils.putConstant(db,self.lastseq_constant,int(sequence_number))
        logger.info(f"Updated constant {self.lastseq_constant} to value {sequence_number}")

    def storePreload(self,db,table,entity_type,entity_meta_dict,df):
        """
        Store the Preload data, to be used by preload override
        """
        import datetime as dt

        # Get the table column names from metadata
        columnMap={d['name']:d['columnName'] for d in entity_meta_dict['dataItems'] if d['type']=='METRIC'}
        logger.debug(f"Column map {pprint.pformat(columnMap)}")
        df.rename(columns=columnMap,inplace=True)

        required_cols = db.get_column_names(table=table, schema=entity_type._db_schema)
        logger.info(f"db columns={required_cols}")
        # keepColumns=['_timestamp']+[v for v in columnMap.values()]+['updated_utc']
        # drop all columns not in the target
        df.drop(columns=[c for c in df.columns if c not in required_cols],inplace=True)
        logger.info(f"df columns keeping required only={[c for c in df.columns]}")

        # Map the column names (we use lower() here because runtime metadata is different from test)
        # logger.info(f"df columns before={[c for c in df.columns]}")
        # df.rename(columns={c:c.lower() for c in df.columns},inplace=True)
        # df.rename(columns={m['name']: m['columnName'] for m in entityMetaDict['dataItemDto']},inplace=True)
        missing_cols = list(set(required_cols) - set(df.columns))
        if len(missing_cols) > 0:
            entity_type.trace_append(created_by=self, msg='AMQP data was missing columns. Adding values.',
                                     log_method=logger.debug, **{'missing_cols': missing_cols})
            logger.info(f"JSON data was missing {len(missing_cols)} columns. Adding values for {missing_cols}")
            for m in missing_cols:
                if m == entity_type._timestamp:
                    df[m] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
                elif m == 'devicetype':
                    df[m] = entity_type.logical_name
                else:
                    logger.info(f"Setting df[{m}] to None")
                    df[m] = None

        logger.info(f"df columns final={[c for c in df.columns]}")
        logger.info(f"Writing df {df.shape} to {table}")
        self.write_frame(df=df, table_name=table)
        entity_type.trace_append(created_by=self, msg='Wrote data to table', log_method=logger.debug, **{'table_name': table, 'schema': entity_type._db_schema, 'row_count': len(df.index)})

        return True