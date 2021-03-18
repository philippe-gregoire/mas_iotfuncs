# *****************************************************************************
# © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# Maximo Monitor IoT Functions to apply a sklearn model
#
# Written by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe
# *****************************************************************************

import os, io, json, importlib, fnmatch
import datetime as dt
import math
import logging,pprint
import inspect
import numpy as np
import pandas as pd

from iotfunctions.base import BaseTransformer, BaseDataSource, BasePreload
from iotfunctions import base, ui, bif, anomaly,estimator

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install
# If forking to a private repository, an access token must be generated
# This is a Personal Access Token for the GitHub ID, who has been authorized to the git project repository
#token='xyz'
#PACKAGE_URL = f"git+https://{token}@github.com/philippe-gregoire/mas_iotfuncs@master"

PACKAGE_URL = f"git+https://github.com/philippe-gregoire/mas_iotfuncs@master"

class ExtendEntityPreload(BasePreload):
    """
        ExtendEntityPreload
        This function extends the EntityData from another Entity, filling-in timestamps
    """
    def __init__(self, source_entity_type_name, ts_interval, ts_lapse, extend_entity_ok):
        logger.info(f"ExtendEntityPreload __init__")
        super().__init__(dummy_items=[], output_item=extend_entity_ok)

        self.source_entity_type_name=source_entity_type_name
        self.ts_interval=ts_interval
        self.ts_lapse=ts_lapse
        self.extend_entity_ok=extend_entity_ok

        # Set this when testing
        self._test_entity_name=None

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        from iotfunctions import ui
        inputs = [
            ui.UISingle(name='source_entity_type_name', datatype=str,
                        description="Name of the entity type that you would like to retrieve data from"),
            ui.UISingle(name='ts_interval', datatype=int,
                        description="Timestamp fill Interval in seconds"),
            ui.UISingle(name='ts_lapse', datatype=int,
                        description="Timestamp lapse in seconds, how far to look in the past for records")
        ]

        # define arguments that behave as function outputs
        outputs = [
            ui.UIStatusFlag(name='extend_entity_ok')
        ]
        return (inputs, outputs)

    def execute(self, df, start_ts=None, end_ts=None, entities=None):
        import iotfunctions.metadata
        from phg_iotfuncs import iotf_utils

        from datetime import datetime, timedelta

        logger.info(f"ExtendEntityData execute({start_ts},{end_ts},{entities})")

        # Extract useful values, entity_type is a iotfunctions.metadata.EntityType object
        from iotfunctions.metadata import EntityType
        entity_type = self.get_entity_type()

        logger.info(f"self._test_entity_name={self._test_entity_name}")
        if self._test_entity_name is not None:
            logger.info(f"Using entity type name {self._test_entity_name} for testing")
            entity_type = entity_type.db.get_entity_type(self._test_entity_name)

        logger.info(f"entity_type type={type(entity_type)} name={entity_type.name} logical_name={entity_type.logical_name}")
        table = entity_type.name
        schema = entity_type._db_schema

        db = entity_type.db
        logger.debug(f"entity_type_metadata keys={db.entity_type_metadata.keys()} ")

        # get entity metadata
        entityMetaDict=db.entity_type_metadata[entity_type.name] if entity_type.name in db.entity_type_metadata else db.entity_type_metadata[entity_type.logical_name]
        logger.debug(f"entityMetaDict={pprint.pformat(entityMetaDict)}")

        params,entity_meta_dict=iotfunctions.metadata.retrieve_entity_type_metadata(_db=db,logical_name=entity_type.logical_name)
        logger.debug(f"entity_meta_dict={pprint.pformat(entity_meta_dict)}")
        logger.debug(f"params={pprint.pformat(params)}")

        logger.debug(f"entity _timestamp={entity_type._timestamp} _timestamp_col={entity_type._timestamp_col}")
        logger.debug(f"entity _data_items={pprint.pformat(entity_type._data_items)}")

        # Set-up time span
        end_ts=datetime.utcnow()
        start_ts=end_ts-timedelta(seconds=self.ts_lapse)

        # get data for this entity
        df = entity_type.get_data(start_ts=start_ts, end_ts=end_ts, entities=None, columns=None)
        logger.info(f"Got this df of len {len(df)} cols={df.columns} from {start_ts} to {end_ts}")
        # drop devicetype columnName
        df=df[[c for c in df.columns if c!='devicetype']]

        # Connect to the source Entity
        sourceEntity = db.get_entity_type(self.source_entity_type_name)
        # cols = [self.key_map_column, sourceEntity._timestamp]
        # cols.extend(self.input_items)
        # renamed_cols = [target._entity_id, target._timestamp]
        # renamed_cols.extend(self.output_items)
        dfSource = sourceEntity.get_data(start_ts=start_ts, end_ts=end_ts, entities=None, columns=None)

        logger.info(f"Got source df of len {len(dfSource)} cols={dfSource.columns} from {start_ts} to {end_ts}")
        if len(dfSource)>0:
            logger.debug(f"dfSource[0]={dfSource.iloc[0].to_dict()}")

        common_cols=[c for c in df.columns if c in dfSource.columns]
        dfSource=dfSource[common_cols]

        # Keep only the data from dfSource that is not already in df, based on index
        keepSource=[i for i in dfSource.index if i not in df.index]
        logger.info(f"Keeping {len(keepSource)} indices {keepSource}")

        if len(keepSource)>0:
            dfSource=dfSource.loc[keepSource]
            dfSource.drop_duplicates(inplace=True)
            logger.info(f"dfSource after filtering len={len(dfSource)}")

            # Add the new data from dfSource to current df
            logger.info(f"Concat two dataframes {common_cols}")
            dfNew = pd.concat([df[common_cols],dfSource[common_cols]], axis=0)
            if len(dfNew)>0:
                logger.debug(f"dfNew[0]={dfNew.iloc[0].to_dict()}")
        else:
            # No new rows to add
            dfNew=df

        # Add back the device type column
        dfNew['devicetype']=entity_type.logical_name

        # generate new values since the last timestamp and now
        dfNew.sort_index(inplace=True)
        ix_latest=dfNew.index[-1]
        ts_latest=ix_latest[1]
        td=(end_ts-ts_latest).total_seconds()
        missing=int(td/self.ts_interval)

        # We base the values on the last Telemetry event, to avoid skipping over
        dfTelemetry=dfNew[dfNew['eventtype']=='Telemetry']
        last_row=dfTelemetry.iloc[-1] if len(dfTelemetry)>0 else dfNew.iloc[-1]

        logger.info(f"latest={ts_latest} elapsed={td}, adding {missing} rows from {last_row.to_dict()}")
        for i in range(1,1+missing):
            ts_fill=ts_latest+timedelta(seconds=i*self.ts_interval)
            new_row=last_row.copy()
            new_row['eventtype']=f"Fill_{self.ts_interval}_sec"
            new_row['_timestamp']=ts_fill
            #new_row['rcv_timestamp_utc']=ts_fill   # Keep TS of original event
            new_row['updated_utc']=datetime.utcnow()
            last_row.name=(ix_latest[0],ts_fill)
            logger.debug(f"append {(ix_latest[0],ts_fill)}={new_row}, len={len(dfNew)}")
            dfNew=dfNew.append(new_row)

        # Write back new data in db
        keepNew=[i for i in dfNew.index if i not in df.index]
        if len(keepNew)>0:
            dfNew=dfNew.loc[keepNew]
            dfNew.drop_duplicates(inplace=True)
            logger.info(f"Writing dfNew {dfNew.shape} to {table}")
            self.write_frame(df=dfNew, table_name=table)
        else:
            logger.info(f"No new row to write")

        return True

class PredictSKLearn(BaseTransformer):
    """
    PredictSKLearn
    Apply a sklearn predict() pipeline to a df
    """

    def __init__(self, model_path, dependent_variables, predicted_value):
        super().__init__()

        # create an instance variable with the same name as each arg
        self.model_path = model_path
        self.dependent_variables=dependent_variables
        self.predicted_value = predicted_value

        # do not do any processing in the init() method. Processing will be done in the execute() method.

    @classmethod
    def build_ui(cls):
        """
        Describe the sklearn model and target column
        """

        # define arguments that behave as function inputs
        inputs = [
            ui.UISingle(required=True, datatype=str, name='model_path', description='Path of sklearn pickle file'),
            ui.UIMultiItem(required=True, datatype=str, name='dependent_variables', description="Columns to provide to the model's predict() function")
        ]

        # define arguments that behave as function outputs
        outputs = [
            ui.UISingle(required=True, datatype=str, name='predicted_value', description='Name of the predicted column')
        ]
        return (inputs, outputs)

    @classmethod
    def get_module_files(cls,pattern):
        module_path=os.path.dirname(importlib.import_module(cls.__module__).__file__)
        logger.debug(f"module_path={module_path}")
        return [f for f in os.listdir(module_path) if fnmatch.fnmatch(f,pattern)],module_path

    def execute(self, df, start_ts=None, end_ts=None, entities=None):
        '''
        '''
        import iotfunctions.metadata

        # Extract useful values
        entity_type = self.get_entity_type()
        logger.info(f"entity_type name={entity_type.name} logical_name={entity_type.logical_name}")

        db = entity_type.db
        logger.debug(f"entity_type_metadata keys={db.entity_type_metadata.keys()} ")

        # get entity metadata
        entityMetaDict=db.entity_type_metadata[entity_type.name] if entity_type.name in db.entity_type_metadata else db.entity_type_metadata[entity_type.logical_name]
        logger.debug(f"Got entityMeta of type {type(entityMetaDict)} value={entityMetaDict}")

        logger.info(f"df shape={df.shape}")
        logger.info(f"df.head(2) ={df.head(2)}")

        import iotfunctions.metadata

        # This class is setup to write to the entity time series table
        table = entity_type.name
        schema = entity_type._db_schema

        import sklearn
        logger.info(f"Running sklearn version {sklearn.__version__}")

        # this returns a pickled model
        try:
            model=db.cos_load(self.model_path, binary=True)
            logger.info(f"Model loaded {model}")
        except Exception as exc:
            logger.error(f"Error loading model from {self.model_path}",exc)
            df[self.predicted_value]=f"ERROR {sklearn.__version__}"

        if model is not None:
            # if dependent_variables have been specified, use for prediction
            dfX=df if self.dependent_variables=='*' else df[self.dependent_variables]
            try:
                logger.info(f"Model predict() on {len(df)} rows and {len(dfX.columns)} columns: {dfX.columns} original: {df.columns}")
                df[self.predicted_value]=model.predict(dfX)
                logger.info(f"Model predicted")
            except Exception as exc:
                logger.error(f"Model predict error",exc)
        else:
            logger.error(f"No model loaded model from {self.model_path}")
            df[self.predicted_value]=f"NoModel {sklearn.__version__}"

        return df
