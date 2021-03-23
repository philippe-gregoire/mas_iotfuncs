# *****************************************************************************
# © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# Maximo Application Suite Analytics Service examples
#
# OSIPi API utilities to get sensor Point values
#
# Author: Philippe Gregoire - IBM in France
# *****************************************************************************
import sys,logging

from iotfunctions import ui
from iotfunctions.base import BaseTransformer

logger = logging.getLogger(__name__)

def getFromPi(srvParams,url=None):
    ''' Issue a GET request to OSPi API
    '''
    import requests, base64

    auth=srvParams.piuser+':'+srvParams.pipass
    hdr={'Authorization': f"Basic {base64.encodebytes(auth.encode())[:-1].decode()}"}
    piurl=url if url else f"https://{srvParams.pihost}:{srvParams.piport}/piwebapi"
    logger.debug(f"Invoking {piurl}")
    resp=requests.request('GET',piurl,verify=False,headers=hdr) # cert=args.picert,
    if not resp.ok:
        resp.reason
        logger.error(f"Error {resp.reason} calling {piurl}")
        raise Exception(resp)
    return resp.json()

def getOSIPiPoints(piHost, piPort, piUser, piPass, pointsNameFilter,pointFields,valueFields):
    ''' Get Point values from OSIPi API server
    '''
    from pprint import pprint
    from argparse import Namespace
    piSrvParams=Namespace(pihost=piHost,piport=piPort,piuser=piUser,pipass=piPass)

    # Navigate to API root
    r_root=getFromPi(piSrvParams)
    if logger.isEnabledFor(logging.DEBUG): pprint(r_root)

    # Navigate to DataServers
    r_datasrvrs=getFromPi(piSrvParams,r_root['Links']['DataServers'])
    if logger.isEnabledFor(logging.DEBUG): pprint(r_datasrvrs)

    TAB='\t'
    def selectedFields(fields,prefix='Items',sep=';'):
        return sep.join([f"{prefix}.{f}" for f in fields])

    # Navigate to Points in first Item
    for datasrv in r_datasrvrs['Items']:
        if logger.isEnabledFor(logging.DEBUG): pprint(datasrv)
        # build the request to get only points matching the provided filter and only selected fields        
        r_points=getFromPi(piSrvParams,f"{datasrv['Links']['Points']}?nameFilter={pointsNameFilter}&selectedFields={selectedFields(pointFields)};Items.Links.RecordedData")
        # pprint(r_points)
        logger.info(f"Found {len(r_points['Items'])} points that match filter {pointsNameFilter}")

        # Dump first point for debugging
        if logger.isEnabledFor(logging.DEBUG): pprint(r_points['Items'][0])

        if logger.isEnabledFor(logging.DEBUG): pprint(getFromPi(piSrvParams,r_points['Items'][0]['Links']['RecordedData']))
        # pprint(getFromPi(args,r_points['Items'][0]['Links']['RecordedData']+'?selectedFields=Items.Value'))
        pointValues={}
        for point in r_points['Items']:
            r_ptvals=getFromPi(piSrvParams,point['Links']['RecordedData']+f"?selectedFields={selectedFields(valueFields)}")
            pointValues[point['Name']]=[{f:v[f] for f in valueFields} for v in r_ptvals['Items']]
            logger.debug(f"{TAB.join([point[f] for f in pointFields])}\t[#{len(r_ptvals['Items'])}]\t= {TAB.join(str(r_ptvals['Items'][-1][f]) for f in valueFields)}")
        return pointValues

def mapValues(ptVals,deviceAttr,point_attr_map):
    """
    Map the values to a flattened version, indexed by timestamp

    For each timestamp and deviceid, we get the corresponding attribute values
    """
    mapped={}

    for ptKey,ptVal in ptVals.items():
        if not ptKey in point_attr_map:
            logger.warning(f"Point key {ptKey} not found in map")
        else:
            attr_name=point_attr_map[ptKey]
            deviceId=attr_name[0]
            attr_name=attr_name[1]
            for row in ptVal:
                ts=row['Timestamp']
                if (ts,deviceId) not in mapped:
                    mapped[(ts,deviceId)]={'Timestamp':ts, deviceAttr:deviceId}
                mapped[(ts,deviceId)][attr_name]=row['Value']

    return mapped

def convertToEntity(ptVals,point_ts_field,entity_date_field,deviceAttr,point_attr_map):
    """
        Convert the raw data to an Entity DataFrame
    """
    import numpy as np, pandas as pd
    import datetime as dt

    # Map values to a flattened version indexed by timestamp
    mapped=mapValues(ptVals,deviceAttr,point_attr_map)

    # We get the messages in an array of dicts, convert to dataframe
    df=pd.DataFrame.from_records([v for v in mapped.values()])
    logger.info(f"df initial columns={[c for c in df.columns]}")

    # Find the date column. We know at this stage that the records we keep have a date_field
    df[point_ts_field]=pd.to_datetime(df[point_ts_field],errors='coerce')

    # Adjust columns, add index columns deviceid, rcv_timestamp_utc
    id_index_col=deviceAttr
    ts_index_col='rcv_timestamp_utc'    # Column which holds the timestamp part of the index
    logger.info(f"Using columns [{deviceAttr},{point_ts_field}] as index [{id_index_col},{ts_index_col}]")
    df.rename(columns={point_ts_field:ts_index_col},inplace=True)
    df.set_index([id_index_col,ts_index_col],drop=False,inplace=True)

    # Set the Date/Timestamp column to the Entity's timestamp column name
    df.rename(columns={ts_index_col:entity_date_field},inplace=True)

    # Adjust column names, set updated_utc to current ts
    # df.rename(columns={'iothub-message-source':'eventtype'},inplace=True)
    df['updated_utc']=dt.datetime.utcnow()

    return df
