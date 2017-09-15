#!/bin/bash -x

if [ "$#" -lt 1 ]; then
	echo "	USAGE: $0 <path/to/client-connect.properties>"
	echo ""
	exit 1
fi
cd `dirname $0`/..

cp=`mvn -q exec:exec -Dexec.executable=echo -Dexec.args="%classpath"`
java -cp "$cp" com.solacesystems.poc.BridgingConnector $*
