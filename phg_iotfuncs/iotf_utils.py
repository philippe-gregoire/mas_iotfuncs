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
