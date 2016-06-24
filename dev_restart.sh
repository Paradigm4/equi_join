#!/bin/bash
#Quick restart script for dev use

DBNAME="mydb"
SCIDB=`which scidb`
SCIDB_INSTALL="`dirname $SCIDB`/.."
iquery -aq "unload_library('rjoin')" > /dev/null 2>&1
set -e
set -x
mydir=`dirname $0`
pushd $mydir
make clean
make SCIDB=$SCIDB_INSTALL SCIDB_THIRDPARTY_PREFIX=/opt/scidb/15.12
scidb.py stopall $DBNAME 
cp librjoin.so $SCIDB_INSTALL/lib/scidb/plugins/
scidb.py startall $DBNAME
#for multinode setups, dont forget to copy to every instance
iquery -aq "load_library('rjoin')"

