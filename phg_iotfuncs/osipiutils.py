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
# OSIPi API utilities to get Elements and Point values
#
# Author: Philippe Gregoire - IBM in France
# *****************************************************************************
import sys,logging

logger = logging.getLogger(__name__)

from phg_iotfuncs import iotf_utils

# Mapping of OSIPi Types to Python types
OSIPI_TYPES_MAP={
    'String': str,
    'Float32': float,
    'Float64': float,
    'Digital': bool,
    'Int32': int,
    'Single': float
}

# Name of the field holding an attribute's timestamp
ATTR_FIELD_TS='Timestamp'
# Name of the field holding an attribute's value
ATTR_FIELD_VAL='Value'

# The default timestamp delta when no starting timestamp is provided
DEFAULT_TIME_DELTA='-30d'

TAB='\t' # for use in f-strings

def plog(msg,level=logging.DEBUG,logger=logger):
    from pprint import pformat
    if logger.isEnabledFor(level):
        logger.log(level,pformat(msg))

def getFromPi(srvParams,url=None,pipath=None,logger=logger):
    ''' Issue a GET request to OSPi API
        srvParams has attributes pihost, piport, piuser, pipass
    '''
    import requests, base64

    auth=srvParams.piuser+':'+srvParams.pipass
    hdr={'Authorization': f"Basic {base64.encodebytes(auth.encode())[:-1].decode()}"}
    piurl=url if url else f"https://{srvParams.pihost}:{srvParams.piport}/piwebapi"
    if pipath: piurl=f"{piurl}/{pipath}"
    dQ='"'
    logger.info(f"Curl equivalent: CURL -k -X GET {' '.join(['-H '+dQ+h+':'+v+dQ for h,v in hdr.items()])} \"{piurl}\"")
    resp=requests.request('GET',piurl,verify=False,headers=hdr) # cert=args.picert,
    if not resp.ok:
        resp.reason
        logger.error(f"Error {resp.reason} calling {piurl}")
        raise Exception(resp)
    return resp.json()

def selectedFields(fields,prefix='Items',sep=';'):
    ''' Helper function '''
    return sep.join([f"{prefix}.{f}" for f in fields])

def _getDataServers(piSrvParams,logger=logger):
    # Navigate to API root
    r_root=getFromPi(piSrvParams,logger=logger)
    plog(r_root,logger=logger)

    # Navigate to DataServers
    r_datasrvrs=getFromPi(piSrvParams,r_root['Links']['DataServers'],logger=logger)
    plog(r_datasrvrs,logger=logger)
    
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

def getOSIPiPoints(piSrvParams, pointsNameFilter,valueFields,logger=logger):
    ''' Get Point values from OSIPi API server
    '''
    r_datasrvrs=_getDataServers(piSrvParams)

    # Navigate to Points in first Item
    for datasrv in r_datasrvrs['Items']:
        plog(datasrv,logger=logger)
        # build the request to get only points matching the provided filter and only selected fields        
        r_points=getFromPi(piSrvParams,f"{datasrv['Links']['Points']}?nameFilter={pointsNameFilter}&selectedFields=Items.Name;Items.PointType;Items.Links.RecordedData")
        # plog(r_points)
        logger.info(f"Found {len(r_points['Items'])} points that match filter {pointsNameFilter}")

        # Dump first point for debugging
        plog(r_points['Items'][0],logger=logger)
        plog(getFromPi(piSrvParams,r_points['Items'][0]['Links']['RecordedData']),logger=logger)

        # Get the values
        pointValues={}
        for point in r_points['Items']:
            r_ptvals=getFromPi(piSrvParams,point['Links']['RecordedData']+f"?selectedFields={selectedFields(valueFields)}")
            pointValues[point['Name']]=[{f:v[f] for f in valueFields} for v in r_ptvals['Items']]
            logger.debug(f"{point['Name']}\t[#{len(r_ptvals['Items'])}]\t= {TAB.join(str(r_ptvals['Items'][-1][f]) for f in valueFields)}")
        return pointValues

def mapPointValues(ptVals,deviceAttr,point_attr_map,logger=logger):
    """
    Map the values from Points to device attributes, indexed by timestamp

    ptVals: array of dict{"Value","Timestamp"} retrieved from OSISoft, indexed by Point name
    For each timestamp and deviceid, we get the corresponding attribute values
    """
    flattened={}

    for ptKey,ptVal in ptVals.items():
        if not point_attr_map or not ptKey in point_attr_map:
            logger.warning(f"Point key {ptKey} not in map")
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

def _getDatabases(piSrvParams,logger=logger):
    # Navigate to API assetservers root
    r_assets=getFromPi(piSrvParams,pipath='assetservers',logger=logger)
    plog(r_assets,logger=logger)

    # Navigate to DataServers
    r_databases=getFromPi(piSrvParams,r_assets['Items'][0]['Links']['Databases'],logger=logger)
    plog(r_databases,logger=logger)

    return r_databases

def listOSIPiElements(piSrvParams,dump_attributes=True,path_prefix=None,_log=print):
    r_databases=_getDatabases(piSrvParams)

    for database in r_databases['Items']:
        if (not path_prefix) or database['Path'].startswith(path_prefix):
            _log(f"{database['Path']}: \"{database['Description']}\"")
        elements=database['Links']['Elements']
        r_elements=getFromPi(piSrvParams,f"{elements}?searchFullHierarchy=true&selectedFields=Items.Path;Items.Description;Items.Links.Elements;Items.Links.Attributes")
        #r_elements=getFromPi(piSrvParams,f"{elements}?searchFullHierarchy=true")
        for element in r_elements['Items']:
            elemPath=element['Path']
            if (not path_prefix) or elemPath.startswith(path_prefix):
                _log(f"{elemPath}: \"{element['Description']}\"")
                if dump_attributes:
                    r_attributes=getFromPi(piSrvParams,element['Links']['Attributes'])
                    for attr in r_attributes['Items']:
                        _log(f"\t{attr['Name']}\ttype={attr['Type']}\tZero={attr['Zero']}\tSpan={attr['Span']}" )

def getParentElements(piSrvParams,parentElementPath,logger=logger):
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
    r_databases=_getDatabases(piSrvParams,logger=logger)

    # Check Path
    elements=None
    for database in r_databases['Items']:
        logger.info(f"path={database['Path']}")
        if parentElementPath.startswith(database['Path']):
            # This is the DB for our element
            elements=database['Links']['Elements']
            break
    # From the elements we want to get the one with the specified name

    #https://192.168.63.39/PIWebAPI/assetdatabases/F1RD2KDQ9HBz10qzW0LwqqvzwAjFsdqRaNDkK5AODV5OilTwT1NJU09GVC1TRVJWRVJcSUJNX0ZBQkxBQg/elements?searchFullHierarchy=true&nameFilter=Motor&selectedFields=Items.Links.Elements
    if not elements:
        return None

    # Get the named Element, parent of sensors
    parentElementName=parentElementPath.split('\\')[-1]
    r_elements=getFromPi(piSrvParams,f"{elements}?searchFullHierarchy=true&selectedFields=Items.Links.Elements;Items.Path&nameFilter={parentElementName}",logger=logger)

    # We get the parent element, now list the children Elements (sensors) within
    #r_elements=getFromPi(piSrvParams,r_elements['Items'][0]['Links']['Elements'])
    for element in r_elements['Items']:
        if element['Path']==parentElementPath:
            r_elements=getFromPi(piSrvParams,f"{element['Links']['Elements']}?selectedFields=Items.Name;Items.Links.RecordedData;Items.Links.InterpolatedData",logger=logger)
            plog(r_elements,logger=logger)

            return r_elements
    
    return None

def getOSIPiElements(piSrvParams, parentElementPath,valueFields,deviceField,startTime=None,interval=None,logger=logger):
    """ Returns a dictionary indexed by (timestamp,deviceid) and the raw json output from the API

        Parameters
        ==========
        PiSrvParams:
            Dictionary of pi server connectivity
        parentElementPath:
            Path from which to get elements
        valueFields:
            Array of fields to retrieve
        deviceField:
            Attribute field containing the device IS
        startTime:
            Starting timestamp
        interval:
            interpolation interval (e.g. '1h', '10s', '1m'). If None, use recorded Data
        logger:
            a logger to use for tracing
    """
    r_elements=getParentElements(piSrvParams, parentElementPath,logger=logger)

    # We will generate a dict indexed by (ts,deviceId)
    sensorValues={}
    if startTime:
        import datetime
        # Format expected by OSIPi API: 2021-04-27T07:29:16.3108215Z, e.g. ISO Format
        if isinstance(startTime,datetime.datetime):
            startTime=startTime.replace(tzinfo=None).isoformat()
        else:
            # attempt to parse date
            try:
                startTime=datetime.date.fromisoformat(startTime)
            except Exception:
                logger.info(f"{startTime} could not be parsed to ISO {datetime.datetime.now().isoformat()}")
    else:
        # Do not specify a startTime
        pass
        # Get all recorded values from 30 days back
        #startTime=DEFAULT_TIME_DELTA
    logger.info(f"Using startTime={startTime}")

    OSIPiRawData={}

    for sensor in r_elements['Items']:
        deviceId=sensor['Name']
        # Select the API URL depending on interpolated or not method
        # use interpolated data
        # see https://192.168.63.39/PIWebAPI/help/controllers/stream/actions/getinterpolated
        # Note that we swap end and start time to get the newest data items first
        # see https://piUser:piPass@piHost/PIWebAPI/help/controllers/streamset/actions/getrecorded
        piurl=sensor['Links']['InterpolatedData' if interval else 'RecordedData']
        # Select appropriate fields
        piurl+=f"?selectedFields=Items.Name;Items.PointType;{selectedFields(valueFields,'Items.Items')}"
        if interval is not None:
            piurl+=f"&interval={interval}"
        if startTime is not None:
            piurl+=f"&boundaryType=Outside&startTime={startTime}"
        r_data=getFromPi(piSrvParams,piurl,logger=logger)
        plog(r_data,logger=logger)
        OSIPiRawData[deviceId]=r_data['Items']
        # Iterate over the Items returned by OSIPi API
        for d in r_data['Items']:
            # Extract the name of the attribute
            attr_name=d['Name']
            #attr_type=d['PointType']
            # Attribute has a list of values in the form of an array of {"Timestamp": ts, "Value": float}
            for item in d['Items']:
                # Get the timestamp and value for this attribute
                ts=item[ATTR_FIELD_TS]
                attr_value=item[ATTR_FIELD_VAL]
                # Note: filter out entries that are of type dict and 
                if not isinstance(attr_value,dict):
                    # If this timestamp for this deviceid has never been seen, initialize it to {"TimeStamp":ts, "DeviceId": id}
                    if (ts,deviceId) not in sensorValues:
                        sensorValues[(ts,deviceId)]={ATTR_FIELD_TS:ts, deviceField:deviceId}
                    # Add the value for this attribute to this (ts,deviceid) entry
                    sensorValues[(ts,deviceId)][attr_name]=item[ATTR_FIELD_VAL]

    return sensorValues,OSIPiRawData

def convertToEntities(flattened,entity_date_field,deviceAttr,logger=logger):
    """
        Convert the raw data to an Entity DataFrame
    """
    import numpy as np, pandas as pd
    import datetime as dt

    tsAttr=ATTR_FIELD_TS

    # We get the messages in an array of dicts, convert to dataframe
    df=pd.DataFrame.from_records([v for v in flattened.values()])

    # Get attributes from OSI
    osiAttrs=[c for c in df.columns]
    logger.info(f"df initial columns={osiAttrs}")


    # Find the date column. We know at this stage that the records we keep have a date_field
    if not tsAttr in osiAttrs:
        logger.warning(f"There is no timestamp column {tsAttr} from the JSON columns")
    else:
        df[tsAttr]=pd.to_datetime(df[tsAttr],errors='coerce')

    # Adjust device_id to remove special chars
    if not deviceAttr in osiAttrs:
        logger.warning(f"There is no device id column {deviceAttr} from the JSON columns")
    else:
        df[deviceAttr]=df[deviceAttr].apply(iotf_utils.toMonitorColumnName)

    # Adjust columns, add index columns deviceid, rcv_timestamp_utc
    id_index_col=deviceAttr
    ts_index_col='rcv_timestamp_utc'    # Column which holds the timestamp part of the index
    logger.debug(f"Using columns [{deviceAttr},{tsAttr}] as index [{id_index_col},{ts_index_col}]")
    df.rename(columns={tsAttr:ts_index_col},inplace=True)
    df.set_index([id_index_col,ts_index_col],drop=False,inplace=True)

    # Set the Date/Timestamp column to the Entity's timestamp column name
    df.rename(columns={ts_index_col:entity_date_field},inplace=True)

    # Adjust column names, set updated_utc to data item timestamp
    df['updated_utc']=df[entity_date_field]

    return df