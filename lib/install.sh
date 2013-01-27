#!/bin/sh

DIR=`pwd`/`dirname $0`
OUTPUT=$DIR/output.log

cd $DIR

# Install osmosis-core
echo "Installing osmosis-core library into the local Maven repository"
mvn install:install-file \
	-Dfile=osmosis-core-0.41.jar \
	-DgroupId=org.openstreetmap \
	-DartifactId=osmosis-core \
	-Dversion=0.41 \
	-Dpackaging=jar >> $OUTPUT
if [ $? -ne 0 ]; then
	echo "Failed: see $OUTPUT logfile for details"
	exit 1
fi

# Install osmosis-core
echo "Installing osmosis-xml library into the local Maven repository"
mvn install:install-file \
	-Dfile=osmosis-xml-0.41.jar \
	-DgroupId=org.openstreetmap \
	-DartifactId=osmosis-xml \
	-Dversion=0.41 \
	-Dpackaging=jar >> $OUTPUT
if [ $? -ne 0 ]; then
	echo "Failed: see $OUTPUT logfile for details"
	exit 1
fi

# Clean
echo "Done! Cleaning working files..."
rm -f $OUTPUT
