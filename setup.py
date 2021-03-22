#!/usr/bin/env python
# *****************************************************************************
# © Copyright IBM Corp. 2021.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
# Main setup to install the package
# To install the package from git, use:
# pip install git+https://github.com/philippe-gregoire/mas_iotfuncs@master

from setuptools import setup, find_packages

setup(name='phg_iotfuncs', version='0.0.9',
      packages=find_packages(),
      #package_data={"phg_iotfuncs":["*.csv"]},
      install_requires=['iotfunctions@git+https://github.com/ibm-watson-iot/functions.git@production','uamqp'],
      extras_require={'kafka': ['confluent-kafka==0.11.5']})
