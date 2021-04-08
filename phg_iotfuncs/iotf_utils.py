# *****************************************************************************
# © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# Maximo Monitor IoT Functions utilities
#
# Written by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe
# *****************************************************************************

import os, io, json, importlib, fnmatch
import datetime as dt
import math
import logging,pprint

logger = logging.getLogger(__name__)

def constantType(entity_name):
    return 'defaultConstants' if entity_name is None else 'constants'

def registerConstant(db,const_name,const_type,const_desc,const_value=None):
    ''' Register a global constant '''
    import iotfunctions.ui
    logger.info(f"Registering constant {const_name} with db={db}")
    constant = iotfunctions.ui.UISingle(name=const_name,description=const_desc,datatype=const_type)

    #Register the constant using the database object
    rc=db.register_constants([constant])
    logger.info(f"Register of constant {const_name} rc={rc}")

    if const_value is not None:
        putConstant(db,const_name,const_value)
    return rc

def getConstant(entity_type_or_db,constant_name,default_value=None,auto_register=False,const_type=int):
    ''' Get a constant for the entity_type or db '''
    import iotfunctions.db

    if isinstance(entity_type_or_db,iotfunctions.db.Database):
        db=entity_type_or_db
        entity_name=None
    else:
        db=entity_type_or_db.db
        entity_name=entity_type.logical_name

    constants=json.loads(db.http_request(constantType(entity_name), entity_name, 'GET',raise_error=True))
    logger.debug(f"{constantType(entity_name)} GET result: {pprint.pformat(constants)}")

    if constant_name is None:
        # Just list all constants
        return {c['name']:c['value']['value'] if 'value' in c and c['value'] is not None and 'value' in c['value'] else None for c in constants}
    else:
        constant_value=default_value
        # Check if constant exists
        constant=[c for c in constants if c['name']==constant_name]
        if len(constant)==0:
            # Does not exist, create it
            if auto_register:
                logger.info(f"Auto-registering {constant_name} of type {const_type}")
                registerConstant(db,constant_name,const_type,f"Auto registered constant {constant_name}",default_value)
        else:
            c=constant[0]
            constant_value=c['value']['value'] if 'value' in c and c['value'] is not None and 'value' in c['value'] else default_value
            logger.debug(f"Got constant {constant_name}={constant_value} from {pprint.pformat(constant)}")

        return constant_value

def putConstant(entity_type_or_db,constant_name,new_value):
    ''' Update (put) a constant for the entity_type or db '''
    import iotfunctions.db

    if isinstance(entity_type_or_db,iotfunctions.db.Database):
        db=entity_type_or_db
        entity_name=None
    else:
        db=entity_type_or_db.db
        entity_name=entity_type.logical_name

    payload={"enabled": True,
             "name": constant_name,
             "value": {"value": new_value}
              # "metadata": {
              #   "additionalProp1": {},
              #   "additionalProp2": {},
              #   "additionalProp3": {}
              # },
            }
    if entity_name is not None:
        payload["entityType"]=entity_name
    logger.debug(f"Putting 'constants' with payload {pprint.pformat(payload)} JSON={json.dumps(payload)}")
    rc=json.loads(db.http_request(constantType(entity_name), entity_name, 'PUT',[payload],raise_error=True))
    logger.info(f"Put request rc={pprint.pformat(rc)}")

    return rc

def toMonitorColumnName(colName):
    ''' Map a column name for Monitor '''
    return colName.replace(' ','_').replace('.','_')

def renameToDBColumns(df,entity_meta_dict):
    """ Rename an entity dataframe to database Column's names
    """
    # Map column names for special characters
    df.rename(columns={c:toMonitorColumnName(c) for c in df.columns},inplace=True)

    # Get the table column names from metadata
    columnMap={d['name']:d['columnName'] for d in entity_meta_dict['dataItems'] if d['type']=='METRIC'}
    logger.info(f"Column map {pprint.pformat(columnMap)}")
    df.rename(columns=columnMap,inplace=True)

def adjustDataFrameColumns(db,entity_meta_dict,df,eventType,force_upper_columns):
    """
    Adjust the raw dataframe columns to match expected format by IoTF DB
    """
    import datetime as dt

    logger.info(f"Adjust: Incoming df columns={df.columns}")
    
    # Extract the columns names required in the DB schema
    db_column_names=db.get_column_names(table=entity_meta_dict['metricsTableName'], schema=entity_meta_dict['schemaName'])

    # List required column names, based on lowercased names
    required_lower_cols =[c.lower() for c in  db_column_names]
    logger.info(f"Required db lowercased columns={required_lower_cols}")

    # user lowercased names for dataframe too, except for keep_case_columns field
    lower_columns_map={c:c.lower() for c in df.columns}
    df.rename(columns=lower_columns_map,inplace=True)

    # drop all columns not in the target
    df.drop(columns=[c for c in df.columns if c not in required_lower_cols],inplace=True)
    logger.info(f"df columns keeping required only={[c for c in df.columns]}")

    missing_cols = list(set(required_lower_cols) - set(df.columns))
    if len(missing_cols) > 0:
        logger.info(f"Missing {len(missing_cols)} columns in incoming dataframe. Adding values for {missing_cols}")
        for m in missing_cols:
            if m == entity_meta_dict['metricTimestampColumn']:
                df[m] = dt.datetime.utcnow() - dt.timedelta(seconds=15)
            elif m == 'devicetype':
                df[m] = entity_meta_dict['entityTypeName']
            elif m == 'eventtype':
                logger.info(f"Setting df[{m}] to {eventType}")
                df[m] = eventType
            else:
                logger.info(f"Setting df[{m}] to None")
                df[m] = None

    # Some columns need to be uppercased, do it now before submitting to storage
    if force_upper_columns and len(force_upper_columns)>0:
        force_upper_columns=[c.upper() for c in force_upper_columns]
        df.rename(columns={c:c.upper() for c in df.columns if c.upper() in force_upper_columns}, inplace=True)

    logger.info(f"df columns final={[c for c in df.columns]}")

    return