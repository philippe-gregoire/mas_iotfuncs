# *****************************************************************************
# # © Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# Extract Maximo Monitor logs from Maximo DB
#
# the credentials_as.json file must be present in the same dir
#
# Written by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe 
# *****************************************************************************

import sys,os, os.path, json
import logging
import sqlalchemy

import utils

# CURRENT_TIMESTAMP='CURRENT TIMESTAMP' # DB2
# CURRENT_DATE='CURRENT DATE' # DB2
CURRENT_TIMESTAMP='CURRENT_TIMESTAMP' # PostGre
CURRENT_DATE='CURRENT_DATE' # PostGre

# WHERE occurred_at >= NOW() - interval '12 hour'

# SELECT_TIME={'day':  f"DATE(UPDATED_TS) < {CURRENT_DATE}",
#         'hour': f"DATE_PART('HOUR',UPDATED_TS) < DATE_PART('HOUR',{CURRENT_TIMESTAMP})",
#         'ever': None
#         }
SELECT_TIME={'hour': f"UPDATED_TS < ({CURRENT_TIMESTAMP} - INTERVAL '1 hour')",
             'day':  f"UPDATED_TS < ({CURRENT_TIMESTAMP} - INTERVAL '24 hour')",
             'today': f"DATE(UPDATED_TS) < {CURRENT_DATE}",
             'ever': None,
             'now': None
            }

SELECT_STATUS=['ERROR','SUCCESS']

def main(argv):
    import argparse

    parser = argparse.ArgumentParser(description='Maximo Monitor Logs handler')
    parser.add_argument('entity_name', type=str,  help='Name of the Entity, or its ID prefixed by #, or *')
    parser.add_argument('-get', type=str,  help='Delete logs older than', choices=SELECT_TIME.keys(),default='None')
    parser.add_argument('-purge', type=str,  help='Delete logs older than', choices=SELECT_TIME.keys(),default='None')
    parser.add_argument('-status', type=str, help='Handle log entries with given status', choices=['*']+SELECT_STATUS,default='ERROR')
    parser.add_argument('-limit', type=int, help='Number of logs to download', default=1)
    parser.add_argument('-filter', type=str, help='Filter to apply on the logs (e.g. phg_iotfuncs)',default=None)
    utils.add_common_args(parser,argv)
    args = parser.parse_args(argv[1:])
    logging.basicConfig(level=args.loglevel)

    db,db_schema=utils.setup_iotfunc(args.creds_file,args.echo_sql)

    if args.entity_name=='*':
        entityMeta={'entityTypeId': '*',
                    'entityTypeName': 'ALL ENTITIES',
                    'schemaName': 'public'}
    elif args.entity_name.startswith('#'):
        entityMeta={'entityTypeId': str(int(args.entity_name[1:])),
                    'entityTypeName':args.entity_name,
                    'schemaName': 'public'}
    elif args.entity_name in db.entity_type_metadata:
        import iotfunctions.metadata
        print(f"fetching Entity {args.entity_name}")
        entity=db.get_entity_type(args.entity_name)
        print(f"Found entity {entity.logical_name} {entity.name}")
        params,entityMeta=iotfunctions.metadata.retrieve_entity_type_metadata(_db=db,logical_name=entity.logical_name)
    else:
        print(f"Entity name {args.entity_name} not found, db keys are {db.entity_type_metadata.keys()}")
        return False

    if args.get in ('day','hour','ever'):
        getLogs(db,entityMeta,"",args.limit,args.filter,selectEntity(entityMeta),selectTime(args.get),selectStatus(args.status))
    elif args.purge in ('day','hour','now'):
        deleteSQL(db,entityMeta,f"Deleting {{count}} records files{' in '+args.status if args.status in SELECT_STATUS else ''} older than {args.purge} for {{entity}}",selectEntity(entityMeta),selectTime(args.purge),selectStatus(args.status))
    else:
        countLogFiles(db,entityMeta,args.status)

def whereAnd(*where):
    where=[w for w in where if (w is not None and len(w)>0)]
    if len(where)==0:
        return ''
    else:
        return ' WHERE ' + (' AND '.join(where))

def execSQL(db,entityMeta,msg,from_stmt,*where):
    stmt=from_stmt+whereAnd(*where)
    # print(stmt)
    res=db.connection.execute(stmt)
    vals=res.fetchone()
    print(msg.format(*vals,entityTypeId=entityMeta['entityTypeId']))
    res.close()

def deleteSQL(db,entityMeta,msg,*where):
    stmt=f"{stmtFrom('SELECT COUNT(*)',entityMeta)} {whereAnd(*where)}"
    # print(stmt)
    resSel=db.connection.execute(stmt)
    vals=resSel.fetchone()
    print(msg.format(entity=entityMeta['entityTypeName'],count=vals[0]))
    resDel=db.connection.execute(f"{stmtFrom('DELETE',entityMeta)} {whereAnd(*where)}")
    print(f"Processed {resDel.rowcount} rows")
    resDel.close()

def getLogs(db,entityMeta,msg,limit,filter,*where):
    stmt=f"{stmtFrom('SELECT logfile_path, status, logfile',entityMeta)} {whereAnd(*where)} ORDER BY updated_ts LIMIT {limit}"
    # print(stmt)
    resSel=db.connection.execute(stmt)
    print(f"Getting {resSel.rowcount} logs")

    vals=resSel.fetchone()
    while vals is not None:
        logPaths=vals[0].split('/')
        entityName=logPaths[1]
        logDate=logPaths[2]
        logTime=logPaths[3].split('.')[0]
        status=vals[1]
        logFile=f"{entityName}_{status}_{logDate}_{logTime}".replace(' ','')
        if filter is not None:
            logFile=f"{logFile}_{filter.split(' ')[0]}"
        logFile=logFile+'.log'
        print(f"Writing {logFile} filtered by {filter}")
        import io
        with io.open(logFile,'w') as F:
            if filter is None:
                F.write(vals[2])
            else:
                # Filter lines
                for l in vals[2].splitlines():
                    if filter in l:
                        F.write(l)
                        F.write(os.linesep)

        # print(msg.format(entity=entityMeta['entityTypeName'],count=vals[0]))
        vals=resSel.fetchone()

def stmtFrom(stmt,entityMeta):
    return f"{stmt} FROM {entityMeta['schemaName']}.KPI_LOGGING"

def selectEntity(entityMeta):
    return '' if entityMeta['entityTypeId']=='*' else f"ENTITY_TYPE_ID = {entityMeta['entityTypeId']}"

def selectTime(timeFilter,reverse=False):
    if reverse:
        return SELECT_TIME[timeFilter].replace('<','>=')
    else:
        return SELECT_TIME[timeFilter]

def selectStatus(status):
    return f"status='{status}'" if status in SELECT_STATUS else ''

def countLogFiles(db,entityMeta,status):
    '''  Count number of log files
    '''
    stmt=f"SELECT COUNT(*), {CURRENT_DATE}, MAX(DATE(UPDATED_TS)), MAX(UPDATED_TS), ({CURRENT_TIMESTAMP}-MAX(UPDATED_TS))"

    if entityMeta['entityTypeId']=='*':
        # Get the ENTITY_TYPE_ID which have logs in the DB
        resSel=db.connection.execute(stmtFrom('SELECT DISTINCT ENTITY_TYPE_ID',entityMeta))
        entityIDs=[]
        vals=resSel.fetchone()
        while vals is not None:
            entityIDs=entityIDs+[str(vals[0])]
            vals=resSel.fetchone()
        print(f"Entity IDs: {', '.join(entityIDs)}")
        # entityMeta['entityTypeId']=', '.join(entityIDs)
    else:
        entityIDs=[entityMeta['entityTypeId']]

    for entityID in entityIDs:
        entityMeta['entityTypeId']=entityID

        statuses=[status] if status in SELECT_STATUS else SELECT_STATUS

        for status in statuses:
            execSQL(db,entityMeta,f"There are {{0}} log files in {status} for entity {{entityTypeId}} at {{1}} latest at {{3}} - {{4}} ago",
                stmtFrom(stmt,entityMeta),selectEntity(entityMeta),selectStatus(status),selectTime('hour',True))

            execSQL(db,entityMeta,f"There are {{0}} log files in {status} for entity {{entityTypeId}} more than one hour ago",
                stmtFrom(stmt,entityMeta),selectEntity(entityMeta),selectStatus(status),selectTime('hour'),selectTime('day',True))

            execSQL(db,entityMeta,f"There are {{0}} log files in {status} for entity {{entityTypeId}} older than one day",
                stmtFrom(stmt,entityMeta),selectEntity(entityMeta),selectStatus(status),selectTime('day'))

if __name__ == "__main__":
    main(sys.argv)
