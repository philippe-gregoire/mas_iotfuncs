# *****************************************************************************
# # © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# Create an Entity from a CSV file
# Parameters: EntityType CSVFile
# Attributes types are determined using pandas dataframe from the CSV file header
#
# the credentials_as.json file must be present in the same dir
#
# Written by Philippe Gregoire, IBM France, Hybrid CLoud Build Team Europe 
# *****************************************************************************

import sys,os, os.path, json

import logging
logger = logging.getLogger(__name__)
from pprint import pprint

import sqlalchemy

# Built-In Functions
#from iotfunctions import bif

#from custom.functions import MaximoAssetHTTPPreload
import iotfunctions

# from custom import settings
import script_utils

def main(argv):
    import argparse

    parser = argparse.ArgumentParser(description='Create Maximo Monitor Entity from CSV file')
    parser.add_argument('entity_name', type=str,  help='Name of the Entity to create')
    parser.add_argument('csv_file', type=str,  help='CSV file to use as input')
    script_utils.add_common_args(parser,argv)
    args = parser.parse_args(argv[1:])
    logging.basicConfig(level=args.loglevel)

    db,db_schema=script_utils.setup_iotfunc(args.creds_file,args.echo_sql)

    script_utils.createEntity(db,db_schema,args.entity_name,script_utils.inferTypesFromCSV(args.csv_file))

if __name__ == "__main__":
    main(sys.argv)