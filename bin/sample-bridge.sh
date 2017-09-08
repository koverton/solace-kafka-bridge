#!/bin/bash
cd `dirname $0`/..

cp=`mvn -q exec:exec -Dexec.executable=echo -Dexec.args="%classpath"`


java -cp "$cp" com.solacesystems.poc.BridgingConnector \
	src/main/resources/localtest.properties
