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

logger = logging.getLogger(__name__)

# Mapping of OSIPi Types to Python types
OSIPI_TYPES_MAP={
    'String': str,
    'Float32': float,
    'Float64': float,
    'Digital': bool,
    'Int32': int,
}

# Name of the field holding an attribute's timestamp
ATTR_FIELD_TS='Timestamp'
# Name of the field holding an attribute's value
ATTR_FIELD_VAL='Value'
# List the attributes fields we want to retrieve
ATTR_FIELDS=[ATTR_FIELD_VAL,ATTR_FIELD_TS]

TAB='\t' # for use in f-strings

def plog(msg,level=logging.DEBUG):
    from pprint import pformat
    if logger.isEnabledFor(level):
        logger.log(level,pformat(msg))

def getFromPi(srvParams,url=None,pipath=None):
    ''' Issue a GET request to OSPi API
        srvParams has attributes pihost, piport, piuser, pipass

    '''
    import requests, base64

    auth=srvParams.piuser+':'+srvParams.pipass
    hdr={'Authorization': f"Basic {base64.encodebytes(auth.encode())[:-1].decode()}"}
    piurl=url if url else f"https://{srvParams.pihost}:{srvParams.piport}/piwebapi"
    if pipath: piurl=f"{piurl}/{pipath}"
    logger.debug(f"Invoking {piurl}")
    resp=requests.request('GET',piurl,verify=False,headers=hdr) # cert=args.picert,
    if not resp.ok:
        resp.reason
        logger.error(f"Error {resp.reason} calling {piurl}")
        raise Exception(resp)
    return resp.json()

def selectedFields(fields,prefix='Items',sep=';'):
    ''' Helper function '''
    return sep.join([f"{prefix}.{f}" for f in fields])

def _getDataServers(piSrvParams):
    # Navigate to API root
    r_root=getFromPi(piSrvParams)
    plog(r_root)

    # Navigate to DataServers
    r_datasrvrs=getFromPi(piSrvParams,r_root['Links']['DataServers'])
    plog(r_datasrvrs)
    
    return r_datasrvrs

def listOSIPiPoints(piSrvParams,_log=print):
    ''' List available Points
    '''
    r_datasrvrs=_getDataServers(piSrvParams)
    
    for datasrv in r_datasrvrs['Items']:
        _log(f"Data Server {datasrv['Name']}")
        r_points=getFromPi(piSrvParams,datasrv['Links']['Points'])
        #r_points=getFromPi(piSrvParams,f"{datasrv['Links']['Points']}?selectedFields=Items.Name;Items.Links.RecordedData")

        for point in r_points['Items']:
            _log(f"{point['Name']}\tType={point['PointType']}\tSpan={point['Span']}\tZero={point['Zero']}")

def getOSIPiPoints(piSrvParams, pointsNameFilter,valueFields):
    ''' Get Point values from OSIPi API server
    '''
    r_datasrvrs=_getDataServers(piSrvParams)

    # Navigate to Points in first Item
    for datasrv in r_datasrvrs['Items']:
        plog(datasrv)
        # build the request to get only points matching the provided filter and only selected fields        
        r_points=getFromPi(piSrvParams,f"{datasrv['Links']['Points']}?nameFilter={pointsNameFilter}&selectedFields=Items.Name;Items.PointType,Items.Links.RecordedData")
        # plog(r_points)
        logger.info(f"Found {len(r_points['Items'])} points that match filter {pointsNameFilter}")

        # Dump first point for debugging
        plog(r_points['Items'][0])
        plog(getFromPi(piSrvParams,r_points['Items'][0]['Links']['RecordedData']))

        # Get the values
        pointValues={}
        for point in r_points['Items']:
            r_ptvals=getFromPi(piSrvParams,point['Links']['RecordedData']+f"?selectedFields={selectedFields(valueFields)}")
            pointValues[point['Name']]=[{f:v[f] for f in valueFields} for v in r_ptvals['Items']]
            logger.debug(f"{point['Name']}\t[#{len(r_ptvals['Items'])}]\t= {TAB.join(str(r_ptvals['Items'][-1][f]) for f in valueFields)}")
        return pointValues

def mapPointValues(ptVals,deviceAttr,point_attr_map):
    """
    Map the values from Points to device attributes, indexed by timestamp

    ptVals: array of dict{"Value","Timestamp"} retrieved from OSISoft, indexed by Point name
    For each timestamp and deviceid, we get the corresponding attribute values
    """
    flattened={}

    for ptKey,ptVal in ptVals.items():
        if not ptKey in point_attr_map:
            logger.warning(f"Point key {ptKey} not found in map")
        else:
            # If an attribute name mapping is required, apply map
            attr_name=point_attr_map[ptKey]
            deviceId=attr_name[0]
            attr_name=attr_name[1]
            for row in ptVal:
                ts=row[ATTR_FIELD_TS]
                if (ts,deviceId) not in flattened:
                    flattened[(ts,deviceId)]={ATTR_FIELD_TS:ts, deviceAttr:deviceId}
                flattened[(ts,deviceId)][attr_name]=row[ATTR_FIELD_VAL]

    return flattened

def _getDatabases(piSrvParams):
    # Navigate to API assetservers root
    r_assets=getFromPi(piSrvParams,pipath='assetservers')
    plog(r_assets)

    # Navigate to DataServers
    r_databases=getFromPi(piSrvParams,r_assets['Items'][0]['Links']['Databases'])
    plog(r_databases)

    return r_databases

def listOSIPiElements(piSrvParams,_log=print):
    r_databases=_getDatabases(piSrvParams)

    for database in r_databases['Items']:
        _log(f"path={database['Name']}: \"{database['Description']}\" => {database['Path']}")
        elements=database['Links']['Elements']
        #r_elements=getFromPi(piSrvParams,f"{elements}?searchFullHierarchy=true&selectedFields=Items.Links.Elements")
        r_elements=getFromPi(piSrvParams,f"{elements}?searchFullHierarchy=true")
        for element in r_elements['Items']:
            _log(f"\t{element['Name']}: {element['Description']} => {element['Path']}")
            r_attributes=getFromPi(piSrvParams,element['Links']['Attributes'])
            for attr in r_attributes['Items']:
                _log(f"\t\t{attr['Name']}\ttype={attr['Type']}\tZero={attr['Zero']}\tSpan={attr['Span']}" )

def getParentElements(piSrvParams,databasePath,elementName):
    ''' Get Element values from OSIPi API server
        Navigation path from API Root:
        - Asset server (https://192.168.63.39/PIWebAPI/assetservers)
        - Items[0]['Links']['Databases']
        -> Items[]['Name']=="IBM_FabLab", Items[]["Path"]: "\\\\OSISOFT-SERVER\\IBM_FabLab"
        -> Items[x]['Links']['Elements'] (GetElements API, Name=Motor, search Hierarchy)
            -> Links['RecordedData']

        So, from the assetservers, we find the Database with given path, then find parent 
        Element with given name, then drill-down through its Elements
        below this will be the motor parts (Entities) from which we get 'RecordedData'
    ''' 
    r_databases=_getDatabases(piSrvParams)

    # Check Path
    elements=None
    for database in r_databases['Items']:
        logger.info(f"path={database['Path']}")
        if databasePath.startswith(database['Path']):
            # This is the DB for our element
            elements=database['Links']['Elements']
            break
    # From the elements we want to get the one with the specified name

    #https://192.168.63.39/PIWebAPI/assetdatabases/F1RD2KDQ9HBz10qzW0LwqqvzwAjFsdqRaNDkK5AODV5OilTwT1NJU09GVC1TRVJWRVJcSUJNX0ZBQkxBQg/elements?searchFullHierarchy=true&nameFilter=Motor&selectedFields=Items.Links.Elements
    if not elements:
        return None

    # Get the named Element, parent of sensors
    r_elements=getFromPi(piSrvParams,f"{elements}?searchFullHierarchy=true&selectedFields=Items.Links.Elements&nameFilter={elementName}")

    # We get the parent element, now list the sensors within
    r_elements=getFromPi(piSrvParams,f"{r_elements['Items'][0]['Links']['Elements']}?selectedFields=Items.Name;Items.PointType,Items.Links.RecordedData")
    plog(r_elements)

    return r_elements

def getOSIPiElements(piSrvParams, databasePath,elementName,valueFields,deviceAttr):
    r_elements=getParentElements(piSrvParams, databasePath,elementName)

    # We want to generate a dict indexed by (ts,deviceId)
    sensorValues={}
    for sensor in r_elements['Items']:
        deviceId=sensor['Name']
        r_data=getFromPi(piSrvParams,f"{sensor['Links']['RecordedData']}?selectedFields=Items.Name;Items.PointType;{selectedFields(valueFields,'Items.Items')}")
        plog(r_data)
        for d in r_data['Items']:
            attr_name=d['Name']
            attr_type=d['PointType']
            for item in d['Items']:
                ts=item[ATTR_FIELD_TS]
                if (ts,deviceId) not in sensorValues:
                    sensorValues[(ts,deviceId)]={ATTR_FIELD_TS:ts, deviceAttr:deviceId}
                sensorValues[(ts,deviceId)][attr_name]=item[ATTR_FIELD_VAL]

    return sensorValues

def convertToEntities(flattened,entity_date_field,deviceAttr):
    """
        Convert the raw data to an Entity DataFrame
    """
    import numpy as np, pandas as pd
    import datetime as dt

    # We get the messages in an array of dicts, convert to dataframe
    df=pd.DataFrame.from_records([v for v in flattened.values()])
    logger.info(f"df initial columns={[c for c in df.columns]}")

    # Find the date column. We know at this stage that the records we keep have a date_field
    df[ATTR_FIELD_TS]=pd.to_datetime(df[ATTR_FIELD_TS],errors='coerce')

    # Adjust columns, add index columns deviceid, rcv_timestamp_utc
    id_index_col=deviceAttr
    ts_index_col='rcv_timestamp_utc'    # Column which holds the timestamp part of the index
    logger.info(f"Using columns [{deviceAttr},{ATTR_FIELD_TS}] as index [{id_index_col},{ts_index_col}]")
    df.rename(columns={ATTR_FIELD_TS:ts_index_col},inplace=True)
    df.set_index([id_index_col,ts_index_col],drop=False,inplace=True)

    # Set the Date/Timestamp column to the Entity's timestamp column name
    df.rename(columns={ts_index_col:entity_date_field},inplace=True)

    # Adjust column names, set updated_utc to current ts
    # df.rename(columns={'iothub-message-source':'eventtype'},inplace=True)
    df['updated_utc']=dt.datetime.utcnow()

    return df
