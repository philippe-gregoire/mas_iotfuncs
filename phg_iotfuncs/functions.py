# *****************************************************************************
# © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************#
# Maximo Monitor IoT Test Functions
# Includes a CSV preload, HelloWorld for testing
#
# Written by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe
# *****************************************************************************

import os, io, json, importlib, fnmatch
import datetime as dt
import math
import logging
import inspect
import numpy as np
import pandas as pd

from sqlalchemy.sql.sqltypes import TIMESTAMP, VARCHAR
from iotfunctions.base import BaseTransformer, BaseDataSource, BasePreload
from iotfunctions import ui

logger = logging.getLogger(__name__)

# Specify the URL to your package here.
# This URL must be accessible via pip install
# If forking to a private repository, an access token must be generated
# This is a Personal Access Token for the GitHub ID, who has been authorized to the git project repository
#token='xyz'
#PACKAGE_URL = f"git+https://{token}@github.com/philippe-gregoire/mas_iotfuncs@master"

PACKAGE_URL = f"git+https://github.com/philippe-gregoire/mas_iotfuncs@master"

class HelloWorldPhG(BaseTransformer):
    '''
    HelloWorldPhG:
    The docstring of the function will show as the function description in the UI.
    '''

    def __init__(self, name, greeting_col):
        # a function is expected to have at least one parameter that acts
        # as an input argument, e.g. "name" is an argument that represents the
        # name to be used in the greeting. It is an "input" as it is something
        # that the function needs to execute.

        # a function is expected to have at lease one parameter that describes
        # the output data items produced by the function, e.g. "greeting_col"
        # is the argument that asks what data item name should be used to
        # deliver the functions outputs

        # always create an instance variable with the same name as your arguments

        self.name = name
        self.greeting_col = greeting_col
        super().__init__()

        # do not place any business logic in the __init__ method  # all business logic goes into the execute() method or methods called by the  # execute() method

    def execute(self, df):
        # the execute() method accepts a dataframe as input and returns a dataframe as output
        # the output dataframe is expected to produce at least one new output column

        df[self.greeting_col] = 'Hello %s' % self.name

        # If the function has no new output data, output a status_flag instead
        # e.g. df[<self.output_col_arg>> = True

        return df

    @classmethod
    def build_ui(cls):
        # Your function will UI built automatically for configuring it
        # This method describes the contents of the dialog that will be built
        # Account for each argument - specifying it as a ui object in the "inputs" or "outputs" list

        inputs = [ui.UISingle(name='name', datatype=str, description='Name of person to greet')]
        outputs = [
            ui.UIFunctionOutSingle(name='greeting_col', datatype=str, description='Output item produced by function')]
        return (inputs, outputs)

class HTTPPreload(BasePreload):
    """
    HTTPPreload
    Do a HTTP request as a preload activity. Load results of the get into the Entity Type time series table.
    HTTP request is experimental
    """

    out_table_name = None

    def __init__(self, request, url, headers=None, body=None, column_map=None, output_item='http_preload_done'):

        if body is None:
            body = {}

        if headers is None:
            headers = {}

        if column_map is None:
            column_map = {}

        super().__init__(dummy_items=[], output_item=output_item)

        # create an instance variable with the same name as each arg

        self.url = url
        self.request = request
        self.headers = headers
        self.body = body
        self.column_map = column_map

        # do not do any processing in the init() method. Processing will be done in the execute() method.

    def execute(self, df, start_ts=None, end_ts=None, entities=None):
        entity_type = self.get_entity_type()
        db = entity_type.db
        encoded_body = json.dumps(self.body).encode('utf-8')
        encoded_headers = json.dumps(self.headers).encode('utf-8')

        # This class is setup to write to the entity time series table
        # To route data to a different table in a custom function,
        # you can assign the table name to the out_table_name class variable
        # or create a new instance variable with the same name

        if self.out_table_name is None:
            table = entity_type.name
        else:
            table = self.out_table_name

        schema = entity_type._db_schema

        # There is a a special test "url" called internal_test
        # Create a dict containing random data when using this
        if self.url == 'internal_test':
            rows = 3
            response_data = {}
            (metrics, dates, categoricals, others) = db.get_column_lists_by_type(table=table, schema=schema,
                                                                                 exclude_cols=[])
            for m in metrics:
                response_data[m] = np.random.normal(0, 1, rows)
            for d in dates:
                response_data[d] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
            for c in categoricals:
                response_data[c] = np.random.choice(['A', 'B', 'C'], rows)

        # make an http request
        else:
            response = db.http.request(self.request, self.url, body=encoded_body, headers=self.headers)
            response_data = response.data.decode('utf-8')
            response_data = json.loads(response_data)

        df = pd.DataFrame(data=response_data)

        # align dataframe with data received

        # use supplied column map to rename columns
        df = df.rename(self.column_map, axis='columns')
        # fill in missing columns with nulls
        required_cols = db.get_column_names(table=table, schema=schema)
        missing_cols = list(set(required_cols) - set(df.columns))
        if len(missing_cols) > 0:
            kwargs = {'missing_cols': missing_cols}
            entity_type.trace_append(created_by=self, msg='http data was missing columns. Adding values.',
                                     log_method=logger.debug, **kwargs)
            for m in missing_cols:
                if m == entity_type._timestamp:
                    df[m] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
                elif m == 'devicetype':
                    df[m] = entity_type.logical_name
                else:
                    df[m] = None

        # remove columns that are not required
        df = df[required_cols]

        # write the dataframe to the database table
        self.write_frame(df=df, table_name=table)
        kwargs = {'table_name': table, 'schema': schema, 'row_count': len(df.index)}
        entity_type.trace_append(created_by=self, msg='Wrote data to table', log_method=logger.debug, **kwargs)

        return True

    @classmethod
    def build_ui(cls):
        """
        Registration metadata
        """
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UISingle(name='request', datatype=str, description='comma separated list of entity ids',
                                  values=['GET', 'POST', 'PUT', 'DELETE']))
        inputs.append(ui.UISingle(name='url', datatype=str, description='request url', tags=['TEXT'], required=True))
        inputs.append(ui.UISingle(name='headers', datatype=dict, description='request url', required=False))
        inputs.append(ui.UISingle(name='body', datatype=dict, description='request body', required=False))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UIStatusFlag(name='output_item'))
        return (inputs, outputs)

class MergeSampleTimeSeries(BaseDataSource):
    """
    MergeSampleTimeSeries

    Merge the contents of a table containing time series data with entity source data
    """
    merge_method = 'outer'  # or outer, concat, nearest
    # use concat when the source time series contains the same metrics as the entity type source data
    # use nearest to align the source time series to the entity source data
    # use outer to add new timestamps and metrics from the source
    merge_nearest_tolerance = pd.Timedelta('1D')
    merge_nearest_direction = 'nearest'
    source_table_name = 'sample_time_series'
    source_entity_id = 'deviceid'
    # metadata for generating sample
    sample_metrics = ['temp', 'pressure', 'velocity']
    sample_entities = ['entity1', 'entity2', 'entity3']
    sample_initial_days = 3
    sample_freq = '1min'
    sample_incremental_min = 5

    def __init__(self, input_items, output_items=None):
        super().__init__(input_items=input_items, output_items=output_items)

    def get_data(self, start_ts=None, end_ts=None, entities=None):

        self.load_sample_data()
        (query, table) = self._entity_type.db.query(self.source_table_name, schema=self._entity_type._db_schema)
        if not start_ts is None:
            query = query.filter(table.c[self._entity_type._timestamp] >= start_ts)
        if not end_ts is None:
            query = query.filter(table.c[self._entity_type._timestamp] < end_ts)
        if not entities is None:
            query = query.filter(table.c.deviceid.in_(entities))

        parse_dates = [self._entity_type._timestamp]
        df = self._entity_type.db.read_sql_query(query.statement, parse_dates=parse_dates)

        return df

    @classmethod
    def get_item_values(cls, arg, db=None):
        """
        Get list of values for a picklist
        """
        if arg == 'input_items':
            if db is None:
                db = cls._entity_type.db
            return db.get_column_names(cls.source_table_name)
        else:
            msg = 'No code implemented to gather available values for argument %s' % arg
            raise NotImplementedError(msg)

    def load_sample_data(self):

        if not self._entity_type.db.if_exists(self.source_table_name):
            generator = TimeSeriesGenerator(metrics=self.sample_metrics, ids=self.sample_entities,
                                            freq=self.sample_freq, days=self.sample_initial_days,
                                            timestamp=self._entity_type._timestamp)
        else:
            generator = TimeSeriesGenerator(metrics=self.sample_metrics, ids=self.sample_entities,
                                            freq=self.sample_freq, seconds=self.sample_incremental_min * 60,
                                            timestamp=self._entity_type._timestamp)

        df = generator.execute()
        self._entity_type.db.write_frame(df=df, table_name=self.source_table_name, version_db_writes=False,
                                         if_exists='append', schema=self._entity_type._db_schema,
                                         timestamp_col=self._entity_type._timestamp_col)

    def get_test_data(self):

        generator = TimeSeriesGenerator(metrics=['acceleration'], ids=self.sample_entities, freq=self.sample_freq,
                                        seconds=300, timestamp=self._entity_type._timestamp)

        df = generator.execute()
        df = self._entity_type.index_df(df)
        return df

    @classmethod
    def build_ui(cls):

        # define arguments that behave as function inputs
        inputs = []
        inputs.append(
            ui.UIMulti(name='input_items', datatype=str, description='Choose columns to bring from source table',
                       required=True, output_item='output_items', is_output_datatype_derived=True))
        # outputs are included as an empty list as they are derived from inputs
        return (inputs, [])

class CSVDataSource(BaseDataSource):
    """
    CSVDataSource

    Get time series data from a CSV file embedded in the git
    """

    is_deprecated = True

    merge_method = 'outer'
    allow_projection_list_trim = False

#     def __init__(self, csv_file, input_items, output_items=None):
    def __init__(self, input_items,output_items):
        #warnings.warn('GetEntityData is deprecated.', DeprecationWarning)
#         super().__init__(input_items=input_items, output_items=output_items)
        #super().__init__(input_items=None, output_items=output_items)
        self.input_items=input_items
        self.output_items=output_items
        self.csv_file = input_items[0]

    def get_data(self, start_ts=None, end_ts=None, entities=None):
        logger.info(f"get_data start_ts={start_ts}, end_ts={end_ts}, entities={entities}")
        db = self.get_db()
        target = self.get_entity_type()

        # load the CSV file into a dataframe
        csv_files,module_path = self.__class__.get_module_files(self.csv_file)
        logger.info(f"csv_files={csv_files}")

        if len(csv_files)==0:
            logger.error(f"Not found in package: {self.csv_file} in {csv_files}")
            allfiles,module_path = self.__class__.get_module_files('*')
            logger.error(f"All files in {module_path}:",allfiles)
            df=None
        else:
            df=pd.read_csv(os.path.join(module_path,csv_files[0]))
            logger.info(f"Loaded df=",df.dtypes,df)
            df['Date']=pd.to_datetime(df['Date'])

            # Adjust columns, add evt_timestamp, deviceid
            df['id']=csv_files[0].split('.')[0]
            df.rename(columns={'Date':'evt_timestamp'},inplace=True)
            df.set_index(['id','evt_timestamp'],drop=False,inplace=True)
            df.rename(columns={'id':'deviceid','evt_timestamp':'_timestamp'},inplace=True)
            logger.info(f"New df=",df.dtypes,df)

        return df

    @classmethod
    def get_module_files(cls,pattern):
        module_path=os.path.dirname(importlib.import_module(cls.__module__).__file__)
        logger.debug(f"module_path={module_path}")
        return [f for f in os.listdir(module_path) if fnmatch.fnmatch(f,pattern)],module_path

    @classmethod
    def build_ui(cls):
        # define arguments that behave as function inputs
        logger.info('build_ui')

        # list the CSV files present
        csv_files,module_path = cls.get_module_files('*.csv')
        logger.info(f"csv_files={csv_files}")

        inputs = []
        inputs.append(ui.UIMulti(name='input_items', datatype=str,
                              #description="Comma separated list of data item names to retrieve from the source entity type",
                              description=f"Enter the name of the embedded CSV data source, one of {csv_files}",
                              output_item='output_items', is_output_datatype_derived=True))
        return (inputs, [])

class CSVPreload(BasePreload):
    """
    CSVPreload
    Do a CSV read as a preload activity. Load results of the get into the Entity Type time series table.
    """

    out_table_name = None

    def __init__(self, csv_file, rebaseTS, output_item='csv_preload_done'):
        super().__init__(dummy_items=[], output_item=output_item)

        # create an instance variable with the same name as each arg

        self.csv_file = csv_file
        self.rebaseTS = rebaseTS

        # do not do any processing in the init() method. Processing will be done in the execute() method.

    @classmethod
    def get_module_files(cls,pattern):
        module_path=os.path.dirname(importlib.import_module(cls.__module__).__file__)
        logger.debug(f"module_path={module_path}")
        return [f for f in os.listdir(module_path) if fnmatch.fnmatch(f,pattern)],module_path

    def execute(self, df, start_ts=None, end_ts=None, entities=None):
        entity_type = self.get_entity_type()
        db = entity_type.db

        # This class is setup to write to the entity time series table
        table = entity_type.name
        schema = entity_type._db_schema

        logger.info(f"entity_type.name={entity_type.name}")
        logger.info(f"entity_type.logical_name={entity_type.logical_name}")
        logger.info(f"keys={db.entity_type_metadata.keys()} ")

        # get entity metadata
        entityMeta=db.entity_type_metadata[entity_type.name] if entity_type.name in db.entity_type_metadata else db.entity_type_metadata[entity_type.logical_name]
        logger.info(f"Got entityMeta={entityMeta} of type {type(entityMeta)}")

        # load the CSV file into a dataframe
        csv_files,module_path = self.__class__.get_module_files(self.csv_file)
        logger.info(f"csv_files={csv_files}")

        if len(csv_files)==0:
            logger.error(f"Not found in package: {self.csv_file} in {csv_files}")
            allfiles,module_path = self.__class__.get_module_files('*')
            logger.error(f"All files in {module_path}:",allfiles)

            return False

        else:
            logger.info(f"Reading CSV file {csv_files[0]}")
            df=pd.read_csv(os.path.join(module_path,csv_files[0]))

            # Adjust date
            df['Date']=pd.to_datetime(df['Date'])

            # Adjust timestamp
            if self.rebaseTS:
                deltaTS=dt.datetime.utcnow()-df['Date'].max()
                logger.info(f"Rebasing timestamp to delta {deltaTS}")
                df['Date']=deltaTS+df['Date']

            # Adjust columns, add evt_timestamp, deviceid
            df['id']=csv_files[0].split('.')[0]
            df.rename(columns={'Date':'evt_timestamp'},inplace=True)
            df.set_index(['id','evt_timestamp'],drop=False,inplace=True)
            df.rename(columns={'id':'deviceid','evt_timestamp':'_timestamp'},inplace=True)
            df['updated_utc']=dt.datetime.utcnow()

            # Map the column names (we use lower() here because runtime metadata is different from test)
            logger.info(f"df columns before={df.columns}")
            df.rename(columns={c:c.lower() for c in df.columns},inplace=True)
            # df.rename(columns={m['name']: m['columnName'] for m in entityMeta['dataItemDto']},inplace=True)
            logger.info(f"df columns after={df.columns}")

            # fill in missing columns with nulls
            required_cols = db.get_column_names(table=table, schema=schema)
            missing_cols = list(set(required_cols) - set(df.columns))
            if len(missing_cols) > 0:
                kwargs = {'missing_cols': missing_cols}
                entity_type.trace_append(created_by=self, msg='CSV data was missing columns. Adding values.',
                                         log_method=logger.debug, **kwargs)
                logger.info(f"CSV data was missing {len(missing_cols)} columns. Adding values. {missing_cols}")
                for m in missing_cols:
                    if m == entity_type._timestamp:
                        df[m] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
                    elif m == 'devicetype':
                        df[m] = entity_type.logical_name
                    else:
                        logger.info(f"Setting df[{m}] to None")
                        df[m] = None

            # remove columns that are not required
            df = df[required_cols]

            # write the dataframe to the database table
            logger.info(f"Writing df {df.shape} to {table}")
            logger.info(f"Writing df columns: {df.columns}")
            self.write_frame(df=df, table_name=table)
            kwargs = {'table_name': table, 'schema': schema, 'row_count': len(df.index)}
            entity_type.trace_append(created_by=self, msg='Wrote data to table', log_method=logger.debug, **kwargs)

            return True

    @classmethod
    def build_ui(cls):
        """
        Registration metadata
        """
        # define arguments that behave as function inputs
        inputs = []
        inputs.append(ui.UISingle(name='csv_file', datatype=str, description='CSV File pattern (*.csv)', tags=['TEXT'], required=True))
        inputs.append(ui.UISingle(name='rebaseTS', datatype=bool, description='Rebase timestamps', required=True))
        # define arguments that behave as function outputs
        outputs = []
        outputs.append(ui.UIStatusFlag(name='output_item'))
        return (inputs, outputs)
