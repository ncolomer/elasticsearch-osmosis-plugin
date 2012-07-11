#!/bin/sh

DIR=`pwd`/`dirname $0`
cd $DIR/

LIB_DIR=$DIR/lib
OUTPUT=$DIR/output.log

# Install osmosis-core
echo "Installing osmosis-core library into the local Maven repository"
cd $LIB_DIR
mvn install:install-file \
	-Dfile=osmosis-core-0.40.1.jar \
	-DgroupId=org.openstreetmap \
	-DartifactId=osmosis-core \
	-Dversion=0.40.1 \
	-Dpackaging=jar >> $OUTPUT
if [ $? -ne 0 ]; then
	echo "Failed: see $OUTPUT logfile for details"
	exit 1
fi

# Clean
echo "Cleaning working files"
rm -f $OUTPUT
