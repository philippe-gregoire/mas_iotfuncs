# *****************************************************************************
# © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# Utility function to get the DB2 server certificate
#
# This is used to extract the DB2 server certificate for 
#
# Author: Philippe Gregoire - IBM in France
# *****************************************************************************

import script_utils
import ssl, io, os

# Load Db2 credentials from config
creds=script_utils.load_creds_file(__file__,'credentials_as')

# Obtain the server certificate
db2Host=creds['db2']['host']
db2Port=creds['db2']['port']

# Write certificate to .pem file
cert,certFile=script_utils.dumpCertificate(db2Host,db2Port,__file__,'DB2')

print(cert)
