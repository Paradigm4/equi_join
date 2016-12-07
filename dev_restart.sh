#!/bin/bash
#Quick restart script for dev use

DBNAME="mydb"
SCIDB=`which scidb`
SCIDB_INSTALL="`dirname $SCIDB`/.."
iquery -aq "unload_library('equi_join')" > /dev/null 2>&1
set -e
set -x
mydir=`dirname $0`
pushd $mydir
make clean
make SCIDB=$SCIDB_INSTALL SCIDB_THIRDPARTY_PREFIX=/opt/scidb/16.9
scidb.py stopall $DBNAME 
cp libequi_join.so $SCIDB_INSTALL/lib/scidb/plugins/
scidb.py startall $DBNAME
#for multinode setups, dont forget to copy to every instance
iquery -aq "load_library('equi_join')"

