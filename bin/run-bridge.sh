#!/bin/bash -x

if [ "$#" -lt 3 ]; then
	echo "	USAGE: $0 <solace-bridge-queue-to-kafka> <kafka> <topic> <list> <..."
	echo ""
	exit 1
fi
cd `dirname $0`/..

cp=`mvn -q exec:exec -Dexec.executable=echo -Dexec.args="%classpath"`
java -cp "$cp" com.solacesystems.poc.BridgingConnector $1 $2 $3
