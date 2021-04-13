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
# Some utility functions to be used across all scripts
#
# Author: Philippe Gregoire - IBM in France
# *****************************************************************************

import sys, os, io, json

import logging, pprint
logger = logging.getLogger(__name__)

import sqlalchemy

# Built-In Functions
# from iotfunctions import bif

def add_operations(parser,operations):
    parser.add_argument('operation', type=str, help=f"Operation", choices=operations+['test','info','register','create','list_constants','set_constant'], default='info')

def common_operation(args,db,db_schema):
    if args.operation=='info':
        # Gather information on the Monitor instance through its API
        pass
    elif args.operation=='list_constants':
        from phg_iotfuncs import iotf_utils

        # get a list of all constants
        print(f"Getting a list of constants")
        pprint.pprint(iotf_utils.getConstant(db, constant_name=None))

    elif args.operation=='set_constant':
        # Put a constant
        from phg_iotfuncs import iotf_utils
        k_name=args.lastseq_constant
        k_desc='PhG Konst'
        try:
            rc=iotf_utils.registerConstant(db,k_name,int,k_desc)
        except:
            pass
        k_val=iotf_utils.getConstant(db,k_name,-1)
        print(f"Got value {k_val}")
        rc=iotf_utils.putConstant(db,k_name,k_val+1)
        k_newval=iotf_utils.getConstant(db,k_name)
        print(f"Got new value {k_newval}")

    elif args.operation=='constant_test':
        # constant API test
        import iotfunctions.ui
        constants = [iotfunctions.ui.UISingle(name='phg_const',description='PhG Konst',datatype=int)]
        payload = []
        for c in constants:
            meta = c.to_metadata()
            name = meta['name']
            default = meta.get('value', None)
            del meta['name']
            try:
                del meta['value']
            except KeyError:
                pass
            payload.append({'name': name, 'entityType': None, 'enabled': True, 'value': default, 'metadata': meta})
        pprint.pprint(payload)
        rc=db.http_request(object_type='defaultConstants', object_name=None, request="POST", payload=payload,
                          raise_error=True)
        pprint.pprint(rc)

def add_common_args(parser,argv):
    ''' Add creds_file and loglevel args '''
    defCredsFile=default_creds_file(argv[0],'credentials_as')
    parser.add_argument('-creds_file', type=str,  help='Maximo Monitor credentials file',default=defCredsFile)
    def type_log_level(level):
        import argparse,logging
        if level.upper() not in logging._nameToLevel:
            raise argparse.ArgumentTypeError(f"{level} is not one of {logging._nameToLevel.keys()}")
        return logging._nameToLevel[level.upper()]
    parser.add_argument('-loglevel', type=type_log_level, help=f"Log Level, one of {logging._nameToLevel.keys()}", choices=logging._nameToLevel.keys(), default='info')
    parser.add_argument('-echo_sql', help=f"Set to trace SQL", action='store_true')

    parser.add_argument('-const_name', type=str, help=f"Name of constant", default=None)
    parser.add_argument('-const_value', type=str, help=f"Value of constant", default=None)
    parser.add_argument('-entityNamePrefix', type=str, help=f"Prefix for Monitor test entity name", default=f"test_entity_for_")

    if not os.path.exists(defCredsFile):
        print(f"WARNING: default credentials file {defCredsFile} does not exist, copy and edit credentials_as.json")

def entity_main(argv,def_entity_name,def_ts_column,columns,column_type=sqlalchemy.Float()):
    ''' Entity creator main method '''
    import argparse

    parser = argparse.ArgumentParser(description=f"Entity Creator with {column_type} columns {columns}")
    parser.add_argument('-entity_name', type=str,  help=f"Name of the Entity, default to '{def_entity_name}'", default=def_entity_name)
    parser.add_argument('-ts_column', type=str,  help=f"timestamp column name, default to '{def_ts_column}'", default=def_ts_column)
    add_common_args(parser,argv)
    args = parser.parse_args(argv[1:])
    logging.basicConfig(level=args.loglevel)

    print(f"Creating Entity {args.entity_name} for tags {columns}")

    db,db_schema=setup_iotfunc(args.creds_file,args.echo_sql)

    columns=[sqlalchemy.Column(args.ts_column,sqlalchemy.DateTime())]+[sqlalchemy.Column(c,column_type) for c in columns]

    createEntity(db,db_schema,args.entity_name,columns,column_type)

def load_creds_file(refPath,prefix):
    creds_file=default_creds_file(refPath,prefix)
    if not os.path.exists(creds_file):
        print(f"Cannot find {creds_file} file")
    import json,io,argparse
    with io.open(creds_file,'r') as f:
        j=json.load(f)
        # return argparse.Namespace(**j)
        return j

def default_creds_file(refPath,prefix):
    ''' Default FQN of the maximo Monitor Analytics service credentials file wrt a given reference file or directory '''
    refPath=os.path.abspath(refPath)
    return os.path.join(os.path.dirname(refPath) if os.path.isfile(refPath) else refPath,f"{prefix}_{os.environ['USERNAME']}.json")

def setup_iotfunc(credsFileName,echoSQL=False,loglevel=logging.INFO):
    ''' Setup the iotfunction database from credentials

        return a tuple made of db, db_schema
    '''
    import iotfunctions, iotfunctions.db

    ## Set up logging level
    # EngineLogging.configure_console_logging(logging.DEBUG)
    iotfunctions.enginelog.EngineLogging.configure_console_logging(loglevel)

    '''
    # Replace with a credentials dictionary or provide a credentials
    # Explore > Usage > Watson IOT Platform Analytics > Copy to clipboard
    # Paste contents in a json file.
    '''
    logger.info(f"Loading Analytics Service credentials from {credsFileName}")
    with open(credsFileName, encoding='utf-8') as credsFile:
        credentials = json.loads(credsFile.read())
    db_schema = None
    if 'postgresql' in credentials:
        credentials['postgresql']['databaseName']=credentials['postgresql']['db']

    if 'db2' in credentials:
        if 'DB_CERTIFICATE_FILE' not in os.environ:
    # Set the DB2 certificate in environment var DB_CERTIFICATE_FILE
            import ssl
            db2Host=credentials['db2']['host']
            db2Port=credentials['db2']['port']

            certFile=os.path.abspath(os.path.join(os.path.dirname(__file__),f"DB2_cert_{db2Port}.pem"))
            if os.path.exists(certFile):
                print(f"Setting DB2 SSL server certificate file to {certFile}")
                os.environ['DB_CERTIFICATE_FILE']=certFile
            else:
                print("DB2 certificate file {certFile} not present")
        else:
            print(f"Using {os.environ['DB_CERTIFICATE_FILE']} certificate file for DB2")

    db = iotfunctions.db.Database(credentials=credentials,echo=echoSQL)

    return db,db_schema

def getCertificateChain(host,port):
    import socket,OpenSSL

    sock = OpenSSL.SSL.Connection(context=OpenSSL.SSL.Context(method=OpenSSL.SSL.TLSv1_2_METHOD), socket=socket.socket(socket.AF_INET, socket.SOCK_STREAM))
    try:
        sock.settimeout(5)
        sock.connect((host, port))
        sock.setblocking(1)
        sock.do_handshake()
        return sock.get_peer_cert_chain()
    finally:
        sock.shutdown()
        sock.close()

def dumpSelfSignedCertificate(host,port,hostType,refPath=None,_logger=print):
    ''' Dump the self-signed certificate which is norally at the root of the chain 
        If refPath is not None, write the certificate PEM file in the directory
    '''
    import io,OpenSSL

    _logger(f"Getting certificate chain for {host}:{port}")
    chain=getCertificateChain(host,port)

    for cert in chain:
        if cert.get_issuer()==cert.get_subject():
            # Self-signed
            _logger(f"Found self-signed certificate for {cert.get_subject()}")
            certPEM=OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM,cert)

            certFile=os.path.abspath(os.path.join(os.path.dirname(refPath),f"{hostType}_cert_{port}.pem")) if refPath else None
            if certFile:
                with io.open(certFile,'wb') as f:
                    _logger(f"Writing {hostType} self-signed certificate to {certFile}")
                    f.write(certPEM)

            return cert,certFile
    # Not found
    return None,None

def inferTypesFromCSV(csv_file):
    '''
    Load a CSV file and generate an array of sqlalchemy.Column
    '''
    import pandas as pd
    import numpy as np
    import sqlalchemy

    # Map np types to sqlalchemy
    TYPEMAP={'float64':sqlalchemy.Float(),'int64':sqlalchemy.Float(),'datetime64[ns]':sqlalchemy.DateTime()}

    df=pd.read_csv(csv_file,parse_dates=['date'],infer_datetime_format=True)
    logger.info(f"Loaded df {df.describe(include='all')}")

    # convert types from numpy to a dict of sqlalchemy types
    csv_columns=[sqlalchemy.Column(k.replace(' ','_').replace('.','_'),TYPEMAP[v.name]) for k,v in df.dtypes.to_dict().items() if v.name in TYPEMAP]
    if len(csv_columns)!=len(df.columns):
        logger.error(f"Some columns could not be mapped: {[(k,v) for k,v in df.dtypes.to_dict().items() if v.name not in TYPEMAP]}")
    else:
        logger.info(f"All columns have been mapped from {csv_file}: {csv_columns}")
    return csv_columns

def createEntity(db,db_schema,entity_name,columns,columnType=sqlalchemy.Float(),date_column='date'):
    '''
    Create an Entity by name and columns
    columns is expected to be an array of sqlalchemy.Column objects. if they are strings, they will be converted to Columns
    '''
    logger.info(f"Creating Entity {entity_name} for {len(columns)} columns")

    import iotfunctions.metadata
    import iotf_utils

    # Align Columns which are not sqlalchemy yet
    columns=[c if isinstance(c,sqlalchemy.Column) else sqlalchemy.Column(iotf_utils.toMonitorColumnName(str(c)),sqlalchemy.TIMESTAMP() if c==date_column else columnType) for c in columns]

    '''
    To do anything with IoT Platform Analytics, you will need one or more entity type.
    You can create entity types through the IoT Platform or using the python API as shown below.
    The database schema is only needed if you are not using the default schema. You can also rename the timestamp.
    '''
    logger.info(f"Dropping Entity table for {entity_name}")
    db.drop_table(entity_name, schema = db_schema)

    entity = iotfunctions.metadata.EntityType(entity_name,
                        db,
                        *columns,
                        **{
                          '_timestamp' : date_column,
                          '_db_schema' : db_schema
                          }
                        )
    # entity = iotfunctions.metadata.BaseCustomEntityType(name=entity_name,db=db,
                        # columns=csv_columns,
                        # constants=,
                        # dimension_columns=,
                        # functions=
                        # )

    '''
    When creating an EntityType object you will need to specify the name of the entity, the database
    object that will contain entity data

    After creating an EntityType you will need to register it so that it visible in the Add Data to Entity Function UI.
    To also register the functions and constants associated with the entity type, specify
    'publish_kpis' = True.
    '''
    from pprint import pprint
    from urllib3.exceptions import HTTPError
    try:
        logger.info(f"Registering Entity {entity}")
        rc=entity.register(raise_error=True,publish_kpis=False)
        if len(rc)>0:
            logger.info(f"Entity registration rc:")
            pprint(json.loads(rc))
    except HTTPError as httpErr:
        logger.error(f"Entity registration rc: {httpErr}")      

def registerFunction(db,db_schema,iot_func):
    logger.info(f"Registering function {iot_func} with db={db}")
    
    rc=db.register_functions([iot_func],raise_error=False)
    if rc=='':
        logger.info(f"Unregistering {iot_func.__name__}")
        db.unregister_functions(iot_func.__name__)
        rc=db.register_functions([iot_func],raise_error=False)
    logger.info(f"register rc={rc}")
