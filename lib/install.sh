#!/bin/sh

DIR=`pwd`/`dirname $0`
OUTPUT=$DIR/output.log
OSMOSIS_VERSION=0.42

cd $DIR

# Install osmosis-core
echo "Installing osmosis-core library into the local Maven repository"
mvn install:install-file \
	-Dfile=osmosis-core-$OSMOSIS_VERSION.jar \
	-DgroupId=org.openstreetmap \
	-DartifactId=osmosis-core \
	-Dversion=$OSMOSIS_VERSION \
	-Dpackaging=jar >> $OUTPUT
if [ $? -ne 0 ]; then
	echo "Failed: see $OUTPUT logfile for details"
	exit 1
fi

# Install osmosis-xml
echo "Installing osmosis-xml library into the local Maven repository"
mvn install:install-file \
	-Dfile=osmosis-xml-$OSMOSIS_VERSION.jar \
	-DgroupId=org.openstreetmap \
	-DartifactId=osmosis-xml \
	-Dversion=$OSMOSIS_VERSION \
	-Dpackaging=jar >> $OUTPUT
if [ $? -ne 0 ]; then
	echo "Failed: see $OUTPUT logfile for details"
	exit 1
fi

# Clean
echo "Done! Cleaning working files..."
rm -f $OUTPUT
